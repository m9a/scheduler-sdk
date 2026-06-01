# How Lightning Metric Reporting Works

## The problem

We need to collect training metrics (loss, accuracy, etc.) from inside Docker containers and send them to the scheduler. In raw PyTorch, every developer writes their own training loop — there is no standard place to intercept metrics. Lightning solves this by wrapping the training loop in a `Trainer` object with a **callback system**.

## How callbacks are injected into user code

- The SDK uses **monkey-patching** — replacing `Trainer.__init__` at runtime so every Trainer the user creates automatically includes our callback.

- This happens in `py-sdk/job_runner/__main__.py`, before the user's script is loaded:

  **Step 1** — Save the original `Trainer.__init__`:
  ```python
  _original_init = pytorch_lightning.Trainer.__init__
  ```

  **Step 2** — Define a wrapper that appends our callback, then calls the original:
  ```python
  def _patched_init(self, *args, **kwargs):
      callbacks = list(kwargs.get("callbacks") or [])  # user's callbacks (or empty)
      callbacks.append(SchedulerCallback(reporter))     # inject ours
      kwargs["callbacks"] = callbacks
      _original_init(self, *args, **kwargs)             # call the real __init__
  ```

  **Step 3** — Replace the original with our wrapper:
  ```python
  pytorch_lightning.Trainer.__init__ = _patched_init
  ```

- When the user's script runs and creates a Trainer, they're calling our wrapper without knowing it:
  ```python
  # User writes this — no SDK imports, no mention of callbacks:
  trainer = Trainer(max_epochs=10, accelerator="auto")

  # What actually runs:
  #   1. _patched_init is called
  #   2. callbacks = []                              (user didn't pass any)
  #   3. callbacks = [SchedulerCallback(reporter)]   (ours is injected)
  #   4. _original_init(self, max_epochs=10, callbacks=[SchedulerCallback(...)])
  ```

## What determines when callbacks are executed

- `Trainer` has a main training loop that calls callback hooks at specific points. Simplified from Lightning's source:

  ```python
  class Trainer:

      def fit(self, model, datamodule):
          datamodule.setup()

          for callback in self.callbacks:
              callback.setup(self, model)                    # hook: setup

          for epoch in range(self.max_epochs):

              # -- training --
              model.train()
              for batch in datamodule.train_dataloader():
                  loss = model.training_step(batch)
                  loss.backward()
                  optimizer.step()

              for callback in self.callbacks:
                  callback.on_train_epoch_end(self, model)   # hook: train epoch done

              # -- validation --
              model.eval()
              for batch in datamodule.val_dataloader():
                  model.validation_step(batch)

              for callback in self.callbacks:
                  callback.on_validation_epoch_end(self, model)  # hook: val epoch done

          for callback in self.callbacks:
              callback.teardown(self, model)                 # hook: cleanup
  ```

- `self.callbacks` is just a list. Lightning iterates it at each hook point and calls the matching method on every callback. Our `SchedulerCallback` is in that list, so it gets called at every epoch boundary.

## What SchedulerCallback does when called

- `SchedulerCallback` extends Lightning's `Callback` base class, which provides no-op defaults for all ~30 hooks. We only override the three epoch-end hooks:

  ```python
  from pytorch_lightning import Callback

  class SchedulerCallback(Callback):

      def __init__(self, reporter):
          super().__init__()
          self._reporter = reporter

      def on_train_epoch_end(self, trainer, pl_module):
          self._report(trainer, "train")

      def on_validation_epoch_end(self, trainer, pl_module):
          self._report(trainer, "val")

      def on_test_epoch_end(self, trainer, pl_module):
          self._report(trainer, "test")
  ```

- When Lightning calls e.g. `on_train_epoch_end`, our callback reads `trainer.callback_metrics` — a dict Lightning maintains with all values from `self.log()` calls in the user's model:

  ```python
  def _report(self, trainer, phase):
      # trainer.callback_metrics contains:
      #   {"train_loss": tensor(0.42), "val_acc": tensor(0.91)}
      metrics = {}
      for key, value in trainer.callback_metrics.items():
          metrics[key] = float(value)

      # POST to the worker agent over HTTP
      self._reporter.report_metrics(trainer.current_epoch, phase, metrics)
  ```

## End-to-end flow

```
User's model                    Lightning Trainer              SchedulerCallback
-----------                     -----------------              -----------------
                                trainer.fit(model, data)
                                  |
training_step(batch):             |
  loss = cross_entropy(...)       |
  self.log("train_loss", loss) ──►│ stores in callback_metrics
  return loss                     |
                                  |
                                epoch ends
                                  |
                                for callback in self.callbacks:
                                  callback.on_train_epoch_end() ──► _report()
                                                                      |
                                                                reads callback_metrics
                                                                      |
                                                                {"train_loss": 0.42}
                                                                      |
                                                                POST /metric-update
                                                                   to worker agent
```

## Why inherit from Callback instead of duck-typing

- Lightning's internal `_call_callback_hooks` uses `getattr(callback, hook_name)` for every lifecycle hook (~30 of them: `setup`, `teardown`, `on_exception`, `state_key`, etc.)
- If any attribute is missing, it raises `AttributeError`
- The `Callback` base class provides no-op defaults for all of them
- By inheriting, we get all those defaults and only override the 3 we care about

## Files involved

| File | Role |
|------|------|
| `py-sdk/job_runner/__main__.py` | Monkey-patches `Trainer.__init__` before user script loads |
| `py-sdk/job_runner/metrics.py` | `SchedulerCallback` — reads metrics, POSTs to worker |
| `py-sdk/job_runner/reporter.py` | `Reporter.report_metrics()` — HTTP POST to worker agent |

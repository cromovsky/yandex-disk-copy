(function () {
  "use strict";
  const modes = new Map();
  for (const mode of ["single", "cross", "local", "onprem"]) {
    const input = document.getElementById("retry-" + mode);
    if (!input) continue;
    const notice = document.getElementById("retry-" + mode + "-notice");
    const state = {input, notice, context: null, serial: 0, busy: false, dirty: false, confirmed: input.checked, intents: new Set()};
    modes.set(mode, state);
    input.addEventListener("change", async () => {
      const context = state.context, desired = input.checked, previous = state.confirmed;
      const serial = ++state.serial;
      state.dirty = true;
      state.intents.forEach((intent) => {if (intent.context === context?.id) intent.enabled = desired;});
      if (notice) notice.textContent = "";
      if (!context?.editable || !context.save) {state.confirmed = desired; return;}
      state.busy = true; input.disabled = true;
      try {
        const result = await context.save(desired);
        if (state.serial !== serial || state.context?.id !== context.id) return;
        if (typeof result?.retry_enabled !== "boolean") throw new Error("invalid_response");
        state.confirmed = result.retry_enabled; input.checked = result.retry_enabled;
      } catch (error) {
        if (state.serial !== serial || state.context?.id !== context.id) return;
        input.checked = previous; state.confirmed = previous;
        if (notice) notice.textContent = context.errorText?.(error) || "Не удалось сохранить повторы. Обновите состояние и повторите действие.";
      } finally {
        if (state.serial === serial && state.context?.id === context.id) {state.busy = false; input.disabled = Boolean(state.context?.loading);}
      }
    });
  }
  window.diskCopyRetries = {
    value(mode) {return modes.get(mode)?.input.checked !== false;},
    track(mode) {
      const state = modes.get(mode);
      const intent = {context: state.context?.id, enabled: state.input.checked,
        close() {state.intents.delete(intent);}};
      state.intents.add(intent);
      return intent;
    },
    bind(mode, context) {
      const state = modes.get(mode);
      if (!state) return;
      if (state.context?.id !== context.id) {
        state.serial += 1; state.busy = false; state.dirty = false;
        state.confirmed = context.enabled !== false; state.input.checked = state.confirmed;
        if (state.notice) state.notice.textContent = "";
      } else if (!state.dirty && !state.busy) {
        state.confirmed = context.enabled !== false; state.input.checked = state.confirmed;
      }
      state.context = context;
      state.input.disabled = state.busy || Boolean(context.loading);
    },
  };
})();

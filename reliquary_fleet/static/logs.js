// Toggle a kind pill on/off → update the hidden kinds input, fire htmx.
  const kindsInput = document.getElementById('kinds-input');
  function syncKinds() {
    const on = Array.from(document.querySelectorAll('#kind-pills .kind-pill[data-on="1"]'))
      .map(p => p.getAttribute('data-kind'));
    kindsInput.value = on.join(',');
    // Re-fire the form so htmx re-fetches with the new value.
    htmx.trigger(document.getElementById('filter-bar'), 'change');
  }
  document.getElementById('kind-pills').addEventListener('click', (e) => {
    const p = e.target.closest('.kind-pill');
    if (!p) return;
    const enabled = p.getAttribute('data-on') !== '1';
    p.setAttribute('data-on', enabled ? '1' : '0');
    p.setAttribute('aria-pressed', String(enabled));
    syncKinds();
  });
  // Pause toggle controls the auto-refresh. While paused the user can
  // freely copy text without the row vanishing under the cursor.
  const pauseBtn = document.getElementById('pause-btn');
  pauseBtn.addEventListener('click', () => {
    const on = pauseBtn.getAttribute('data-paused') === '1';
    pauseBtn.setAttribute('data-paused', on ? '0' : '1');
    pauseBtn.setAttribute('aria-pressed', String(!on));
    pauseBtn.textContent = on ? '● Live' : 'Ⅱ Paused';
  });
  // htmx every-2s trigger checks this each fire — return true to skip.
  window.isPaused = () => pauseBtn.getAttribute('data-paused') === '1';
  // Highlight ours-only rows by adding a class to the table.
  document.body.addEventListener('change', (e) => {
    if (e.target.name === 'ours') {
      const table = document.querySelector('#log-out .log-table');
      if (!table) return;
      if (e.target.checked) table.classList.add('ours-mode');
      else table.classList.remove('ours-mode');
    }
  });
  function enhanceLogTable() {
    const table = document.querySelector('#log-out .log-table');
    if (!table) return;
    table.setAttribute('aria-label', 'Filtered validator event log');
    table.querySelectorAll('thead th').forEach((heading) => {
      heading.setAttribute('scope', 'col');
    });
  }
  document.body.addEventListener('htmx:beforeRequest', () => {
    document.getElementById('log-out').setAttribute('aria-busy', 'true');
  });
  document.body.addEventListener('htmx:afterSwap', () => {
    document.getElementById('log-out').setAttribute('aria-busy', 'false');
    enhanceLogTable();
  });
  // Click a row to copy the raw line to clipboard.
  document.body.addEventListener('click', (e) => {
    const row = e.target.closest('.log-row');
    if (!row) return;
    // Don't trigger when the user is selecting text.
    if (window.getSelection && window.getSelection().toString().length > 0) return;
    const ts = row.querySelector('.log-ts')?.textContent?.trim() ?? '';
    const kind = row.querySelector('.log-kind')?.textContent?.trim() ?? '';
    const reason = row.querySelector('.log-reason-cell')?.textContent?.trim() ?? '';
    const hk = row.querySelector('.log-hk')?.textContent?.trim() ?? '';
    const msg = row.querySelector('.log-msg')?.textContent ?? '';
    const line = `${ts} ${kind} ${reason} ${hk} ${msg}`.trim();
    if (navigator.clipboard) {
      navigator.clipboard.writeText(line).then(() => {
        row.style.background = 'rgba(232,122,62,0.18)';
        setTimeout(() => { row.style.background = ''; }, 320);
      });
    }
  });

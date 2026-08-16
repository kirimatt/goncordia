const esc = value => String(value ?? '').replace(/[&<>"']/g, char => ({
  '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
})[char]);
const urlpart = value => encodeURIComponent(value).replace(/'/g, '%27');

async function api(url, options) {
  const response = await fetch(url, options);
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body.error || response.statusText);
  return body;
}

async function health() {
  try {
    const result = await api('readyz');
    document.querySelector('#health').textContent = `${result.driver} · ready`;
  } catch (_) {
    document.querySelector('#health').textContent = 'unavailable';
  }
}

async function queues() {
  try {
    const page = await api('api/queues');
    document.querySelector('#queues').innerHTML = page.items.map(queue => {
      const action = queue.Paused ? 'resume' : 'pause';
      const stats = queue.stats ? `${esc(queue.stats.total)} jobs` : 'stats unavailable';
      return `<div class="queue"><div><strong>${esc(queue.Name)}</strong><div class="muted">${stats}</div></div><button data-queue="${esc(queue.Name)}" data-action="${action}">${queue.Paused ? 'Resume' : 'Pause'}</button></div>`;
    }).join('') || '<span class="muted">No queues</span>';
  } catch (error) {
    document.querySelector('#error').textContent = error.message;
  }
}

async function queueAction(queue, action) {
  await api(`api/queues/${urlpart(queue)}/${action}`, {
    method: 'POST',
    headers: {'X-Goncordia-Confirm': action}
  });
  await queues();
}

async function jobs() {
  const params = new URLSearchParams(new FormData(document.querySelector('#filters')));
  try {
    const page = await api(`api/jobs?${params}`);
    document.querySelector('#error').textContent = '';
    document.querySelector('#jobs').innerHTML = page.items.map(job => `<tr><td><code>${esc(job.ID)}</code><br>${esc(job.Kind)}</td><td>${esc(job.Queue)}</td><td class="state">${esc(job.State)}</td><td>${esc(job.AttemptNum)} / ${esc(job.MaxRetry)}</td><td><div class="actions"><button data-job="${esc(job.ID)}" data-action="retry">Retry</button><button data-job="${esc(job.ID)}" data-action="cancel">Cancel</button><button data-job="${esc(job.ID)}" data-action="delete">Delete</button></div></td></tr>`).join('');
  } catch (error) {
    document.querySelector('#error').textContent = error.message;
  }
}

async function jobAction(id, action) {
  await api(`api/jobs/${urlpart(id)}/${action}`, {
    method: 'POST',
    headers: {'X-Goncordia-Confirm': action}
  });
  await jobs();
  await queues();
}

document.querySelector('#filters').addEventListener('submit', event => {
  event.preventDefault();
  jobs();
});
document.querySelector('#queues').addEventListener('click', event => {
  const button = event.target.closest('button[data-queue]');
  if (button) queueAction(button.dataset.queue, button.dataset.action).catch(error => {
    document.querySelector('#error').textContent = error.message;
  });
});
document.querySelector('#jobs').addEventListener('click', event => {
  const button = event.target.closest('button[data-job]');
  if (button) jobAction(button.dataset.job, button.dataset.action).catch(error => {
    document.querySelector('#error').textContent = error.message;
  });
});

health();
queues();
jobs();
setInterval(health, 15000);

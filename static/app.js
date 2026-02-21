'use strict';

// ═══════════════════════════════════════════════════
//  WatchFinder — Frontend App
// ═══════════════════════════════════════════════════

const API = {
  listings:     '/api/listings',
  stats:        '/api/stats',
  sources:      '/api/sources',
  scrape:       '/api/scrape',
  scrapeStatus: '/api/scrape/status',
  settings:     '/api/settings',
};

// ── State ──────────────────────────────────────────
let currentView  = 'grid';
let scrapePoller = null;
let year2007Only = false;
let allSources   = [];

// ── DOM refs ───────────────────────────────────────
const $ = (sel, ctx = document) => ctx.querySelector(sel);
const $$ = (sel, ctx = document) => [...ctx.querySelectorAll(sel)];

const grid         = $('#listingsGrid');
const emptyState   = $('#emptyState');
const loadingState = $('#loadingState');
const countLabel   = $('#listingsCount');

// ── Utility ────────────────────────────────────────
function fmt(n) {
  if (n == null) return '—';
  return Number(n).toLocaleString('en-US');
}
function fmtPrice(p) {
  if (p == null || p === 0) return null;
  return '$' + Number(p).toLocaleString('en-US', { minimumFractionDigits: 0 });
}
function badgeClass(rating) {
  return { Great: 'badge-great', Good: 'badge-good', Fair: 'badge-fair', High: 'badge-high' }[rating] || 'badge-na';
}
function escHtml(s) {
  if (!s) return '';
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}
function timeAgo(dateStr) {
  if (!dateStr) return '';
  const d   = new Date(dateStr + (dateStr.includes('Z') ? '' : 'Z'));
  const sec = Math.round((Date.now() - d.getTime()) / 1000);
  if (isNaN(sec) || sec < 0) return '';
  if (sec < 60)    return 'just now';
  if (sec < 3600)  return `${Math.floor(sec/60)}m ago`;
  if (sec < 86400) return `${Math.floor(sec/3600)}h ago`;
  return `${Math.floor(sec/86400)}d ago`;
}

// ── Build filter params ────────────────────────────
function buildParams() {
  const p = new URLSearchParams();
  const q         = $('#fSearch').value.trim();
  const brand     = $('#fBrand').value;
  const yearMin   = $('#fYearMin').value;
  const yearMax   = $('#fYearMax').value;
  const priceMin  = $('#fPriceMin').value;
  const priceMax  = $('#fPriceMax').value;
  const condition = $('#fCondition').value;
  const source    = $('#fSource').value;
  const [sort, order] = $('#fSort').value.split(':');

  if (q)        p.set('q', q);
  if (brand)    p.set('brand', brand);
  if (yearMin)  p.set('year_min', yearMin);
  if (yearMax)  p.set('year_max', yearMax);
  if (priceMin) p.set('price_min', priceMin);
  if (priceMax) p.set('price_max', priceMax);
  if (condition)p.set('condition', condition);
  if (source)   p.set('source', source);
  p.set('sort', sort);
  p.set('order', order);
  $$('.rating-cb:checked').forEach(cb => p.append('rating', cb.value));
  return p;
}

// ── Fetch listings ─────────────────────────────────
async function fetchListings() {
  grid.innerHTML = '';
  emptyState.classList.add('hidden');
  loadingState.classList.remove('hidden');
  countLabel.textContent = 'Loading…';

  try {
    const res  = await fetch(`${API.listings}?${buildParams()}`);
    const data = await res.json();
    loadingState.classList.add('hidden');

    if (!data.listings.length) {
      emptyState.classList.remove('hidden');
      countLabel.textContent = '0 listings';
      return;
    }

    countLabel.textContent = `${data.listings.length} listing${data.listings.length !== 1 ? 's' : ''}`;
    renderCards(data.listings);

    // Populate brand dropdown from results
    const brands = new Set(data.listings.map(l => l.brand).filter(Boolean));
    populateBrandFilter([...brands].sort());

  } catch (err) {
    loadingState.classList.add('hidden');
    emptyState.classList.remove('hidden');
    countLabel.textContent = 'Error loading listings';
    console.error(err);
  }
}

// ── Render cards ───────────────────────────────────
function renderCards(listings) {
  grid.innerHTML = '';
  listings.forEach(l => {
    const card = document.createElement('div');
    card.className = 'watch-card';
    card.dataset.id = l.id;

    const priceStr = fmtPrice(l.price);
    const rating   = l.price_rating;
    const isTarget = l.year === 2007;

    const imageHtml = l.image_url
      ? `<div class="card-image"><img src="${escHtml(l.image_url)}" alt="Watch" loading="lazy" onerror="this.parentNode.innerHTML='⌚'"></div>`
      : `<div class="card-image no-img">⌚</div>`;

    const pills = [
      l.brand     && `<span class="meta-pill">${escHtml(l.brand)}</span>`,
      l.year      && `<span class="meta-pill${isTarget ? ' badge badge-year' : ''}">${l.year}</span>`,
      l.reference && `<span class="meta-pill">${escHtml(l.reference)}</span>`,
      l.condition && `<span class="meta-pill">${escHtml(l.condition)}</span>`,
    ].filter(Boolean).join('');

    const ratingHtml = rating
      ? `<span class="badge ${badgeClass(rating)}">${rating}</span>` : '';

    card.innerHTML = `
      ${imageHtml}
      <div class="card-body">
        <div class="card-title">${escHtml(l.title)}</div>
        <div class="card-meta">${pills}${ratingHtml}</div>
        <div class="card-footer">
          <div>
            <div class="card-price${priceStr ? '' : ' no-price'}">${priceStr || 'Price unlisted'}</div>
            ${l.market_price && isTarget
              ? `<div class="card-market">Market: ${fmtPrice(l.market_price)} · ${l.price_delta_pct}%</div>` : ''}
          </div>
          <span class="card-source">${escHtml(l.source_name || '')}</span>
        </div>
        ${l.date_found ? `<div style="font-size:11px;color:var(--text-muted);margin-top:4px">${timeAgo(l.date_found)}</div>` : ''}
      </div>
    `;
    card.addEventListener('click', () => openDetail(l));
    grid.appendChild(card);
  });
}

// ── Stats ──────────────────────────────────────────
async function fetchStats() {
  try {
    const data = await (await fetch(API.stats)).json();
    $('#statTotal').textContent = fmt(data.total);
    $('#stat2007').textContent  = fmt(data.year_2007);
    $('#statGreat').textContent = fmt(data.ratings?.Great ?? 0);
    $('#statGood').textContent  = fmt(data.ratings?.Good  ?? 0);
    $('#statFair').textContent  = fmt(data.ratings?.Fair  ?? 0);
    $('#statHigh').textContent  = fmt(data.ratings?.High  ?? 0);
  } catch (err) { console.error('Stats fetch failed:', err); }
}

// ── Sources / brand filter ─────────────────────────
function populateBrandFilter(brands) {
  const sel = $('#fBrand');
  const cur = sel.value;
  sel.innerHTML = '<option value="">All brands</option>';
  brands.forEach(b => {
    const opt = document.createElement('option');
    opt.value = b; opt.textContent = b;
    if (b === cur) opt.selected = true;
    sel.appendChild(opt);
  });
}

async function fetchSources() {
  try {
    allSources = await (await fetch(API.sources)).json();
    const sel = $('#fSource');
    allSources.forEach(s => {
      const opt = document.createElement('option');
      opt.value = s.name; opt.textContent = s.name;
      sel.appendChild(opt);
    });
    buildScrapeSourceChecks();
    buildCookieFields();
  } catch (err) { console.error('Sources fetch failed:', err); }
}

function buildScrapeSourceChecks() {
  const container = $('#scrapeSourceChecks');
  if (!container) return;
  container.innerHTML = '';
  allSources.forEach(s => {
    const label = document.createElement('label');
    label.className = 'checkbox-label';
    label.innerHTML = `<input type="checkbox" class="source-cb" value="${escHtml(s.name)}" checked><span>${escHtml(s.name)}</span>`;
    container.appendChild(label);
  });
}

function buildCookieFields() {
  const container = $('#cookieFields');
  if (!container) return;
  container.innerHTML = '<h3>Forum Session Cookies</h3>';
  allSources.forEach(s => {
    const div = document.createElement('div');
    div.className = 'cookie-field';
    div.innerHTML = `
      <label>${escHtml(s.name)}</label>
      <textarea id="cookie_${s.id}" placeholder="Paste cookie string here, e.g.: bb_sessionhash=abc123; bb_userid=456; bb_password=…" rows="2"></textarea>
      <div class="cookie-hint">Get cookies from browser DevTools → Application → Cookies → ${escHtml(s.url)}</div>
    `;
    container.appendChild(div);
  });
}

// ── Detail modal ───────────────────────────────────
function openDetail(l) {
  const isTarget = l.year === 2007;
  const priceStr = fmtPrice(l.price);
  const rating   = l.price_rating;

  const imageHtml = l.image_url
    ? `<img class="detail-image" src="${escHtml(l.image_url)}" alt="Watch">` : '';

  const ratingHtml = rating
    ? `<span class="badge ${badgeClass(rating)}" style="font-size:14px;padding:5px 14px">${rating} Deal</span>`
    : '<span class="badge badge-na">No Rating</span>';

  const marketHtml = (isTarget && l.market_price)
    ? `<div class="market-price-note">Market: ${fmtPrice(l.market_price)} · ${l.price_delta_pct}% of market
       ${l.watchcharts_url ? `· <a href="${escHtml(l.watchcharts_url)}" target="_blank" style="color:var(--accent)">WatchCharts ↗</a>` : ''}</div>`
    : (isTarget ? '<div class="market-price-note">Market price not available</div>' : '');

  const fields = [
    ['Brand',     l.brand],
    ['Model',     l.model],
    ['Reference', l.reference],
    ['Year',      l.year],
    ['Condition', l.condition],
    ['Seller',    l.seller],
    ['Source',    l.source_name],
    ['Found',     l.date_found ? new Date(l.date_found + 'Z').toLocaleDateString() : null],
    ['Listed',    l.date_listed],
  ].filter(([,v]) => v);

  $('#modalBody').innerHTML = `
    ${imageHtml}
    <div class="detail-header">
      <div class="detail-title">${escHtml(l.title)}</div>
      <div class="card-meta" style="gap:6px;margin-top:6px">
        ${isTarget ? '<span class="badge badge-year">2007</span>' : ''}
        ${l.brand ? `<span class="meta-pill">${escHtml(l.brand)}</span>` : ''}
        ${l.reference ? `<span class="meta-pill">${escHtml(l.reference)}</span>` : ''}
        ${l.condition ? `<span class="meta-pill">${escHtml(l.condition)}</span>` : ''}
      </div>
    </div>
    <div class="detail-price-block">
      <div class="detail-price">${priceStr || 'Price unlisted'}</div>
      <div class="detail-rating-block">
        ${ratingHtml}
        ${marketHtml}
      </div>
    </div>
    <div class="detail-grid">
      ${fields.map(([k,v]) => `
        <div class="detail-field">
          <label>${escHtml(k)}</label>
          <span>${escHtml(String(v))}</span>
        </div>`).join('')}
    </div>
    ${l.description ? `<div class="detail-desc">${escHtml(l.description)}</div>` : ''}
    <div class="detail-actions">
      <a href="${escHtml(l.listing_url)}" target="_blank" rel="noopener" class="btn btn-primary">View Listing ↗</a>
      ${l.watchcharts_url ? `<a href="${escHtml(l.watchcharts_url)}" target="_blank" rel="noopener" class="btn btn-secondary">WatchCharts ↗</a>` : ''}
    </div>
  `;

  $('#modal').classList.remove('hidden');
  document.body.style.overflow = 'hidden';
}

function closeModal() {
  $('#modal').classList.add('hidden');
  document.body.style.overflow = '';
}

// ── Scrape ─────────────────────────────────────────
function openScrapeModal() { $('#scrapeModal').classList.remove('hidden'); }
function closeScrapeModal() { $('#scrapeModal').classList.add('hidden'); }

async function startScrape() {
  const pages      = parseInt($('#scrapePages').value) || 3;
  const targetYear = parseInt($('#scrapeTargetYear').value) || 2007;
  const sources    = $$('.source-cb:checked').map(cb => cb.value);

  closeScrapeModal();
  $('#targetYearBadge').textContent = targetYear;

  const statusEl = $('#scrapeStatus');
  statusEl.className  = 'scrape-status running';
  statusEl.textContent = '⟳ Scanning…';
  statusEl.classList.remove('hidden');
  $('#btnScrape').disabled = true;

  try {
    const res = await fetch(API.scrape, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ pages, target_year: targetYear, sources: sources.length ? sources : null }),
    });
    if (!res.ok) throw new Error((await res.json()).error || 'Unknown error');
    if (scrapePoller) clearInterval(scrapePoller);
    scrapePoller = setInterval(pollScrapeStatus, 3000);
  } catch (err) {
    statusEl.className  = 'scrape-status error';
    statusEl.textContent = '✕ Error: ' + err.message;
    $('#btnScrape').disabled = false;
    setTimeout(() => statusEl.classList.add('hidden'), 8000);
  }
}

async function pollScrapeStatus() {
  try {
    const data     = await (await fetch(API.scrapeStatus)).json();
    const statusEl = $('#scrapeStatus');

    if (data.running) {
      statusEl.textContent = '⟳ Scanning…';
      return;
    }

    clearInterval(scrapePoller);
    scrapePoller = null;
    $('#btnScrape').disabled = false;

    if (data.last_result?.error) {
      statusEl.className  = 'scrape-status error';
      statusEl.textContent = '✕ ' + data.last_result.error;
    } else if (data.last_result) {
      const r = data.last_result;
      const blocked = r.blocked ? ' ⚠ Some sources were blocked (Cloudflare). See Settings.' : '';
      statusEl.className  = 'scrape-status done';
      statusEl.textContent = `✓ Done · ${r.new ?? 0} new · ${r.updated ?? 0} updated · ${r.priced ?? 0} priced${blocked}`;
      fetchListings();
      fetchStats();
    }
    setTimeout(() => statusEl.classList.add('hidden'), 12000);
  } catch (err) { console.error('Poll error:', err); }
}

// ── Settings ───────────────────────────────────────
async function openSettings() {
  // Load saved settings
  try {
    const settings = await (await fetch(API.settings)).json();
    if (settings.target_year) $('#settingTargetYear').value = settings.target_year;
    if (settings.scrape_pages) $('#settingPages').value = settings.scrape_pages;
    // Cookie fields are loaded by buildCookieFields() on startup
  } catch (err) { console.error(err); }
  $('#settingsModal').classList.remove('hidden');
}

function closeSettings() { $('#settingsModal').classList.add('hidden'); }

async function saveSettings() {
  const payload = {
    target_year:  $('#settingTargetYear').value,
    scrape_pages: $('#settingPages').value,
  };

  // Gather cookie fields
  allSources.forEach(s => {
    const el = $(`#cookie_${s.id}`);
    if (el && el.value.trim()) {
      payload[`cookie_${s.name}`] = el.value.trim();
    }
  });

  try {
    await fetch(API.settings, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    closeSettings();
    // Apply target year to badge
    if (payload.target_year) $('#targetYearBadge').textContent = payload.target_year;
  } catch (err) {
    alert('Failed to save settings: ' + err.message);
  }
}

// ── View toggle ────────────────────────────────────
function setView(view) {
  currentView = view;
  grid.classList.toggle('list-view', view === 'list');
  $$('.view-btn').forEach(b => b.classList.toggle('active', b.dataset.view === view));
}

// ── Auto-filter on change ──────────────────────────
function setupAutoFilter() {
  $$('select.filter-select, input.filter-input').forEach(el => {
    el.addEventListener('change', fetchListings);
  });
  let debounce;
  $('#fSearch').addEventListener('input', () => {
    clearTimeout(debounce);
    debounce = setTimeout(fetchListings, 400);
  });
  $$('.rating-cb').forEach(cb => cb.addEventListener('change', fetchListings));
}

// ── Boot ───────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  fetchStats();
  fetchSources();
  fetchListings();

  // View toggle
  $$('.view-btn').forEach(btn => btn.addEventListener('click', () => setView(btn.dataset.view)));

  // Scrape controls
  $('#btnScrape').addEventListener('click', openScrapeModal);
  $('#btnScrapeCancel').addEventListener('click', closeScrapeModal);
  $('#btnScrapeConfirm').addEventListener('click', startScrape);
  $('#scrapeModal .modal-backdrop').addEventListener('click', closeScrapeModal);

  // Settings controls
  $('#btnSettings').addEventListener('click', openSettings);
  $('#settingsClose').addEventListener('click', closeSettings);
  $('#btnSettingsCancel').addEventListener('click', closeSettings);
  $('#btnSettingsSave').addEventListener('click', saveSettings);
  $('#settingsModal .modal-backdrop').addEventListener('click', closeSettings);

  // Detail modal
  $('#modalClose').addEventListener('click', closeModal);
  $('#modal .modal-backdrop').addEventListener('click', closeModal);

  // ESC closes any modal
  document.addEventListener('keydown', e => {
    if (e.key === 'Escape') { closeModal(); closeScrapeModal(); closeSettings(); }
  });

  // Clear filters
  $('#btnClearFilters').addEventListener('click', () => {
    $('#fSearch').value = $('#fBrand').value = $('#fYearMin').value =
    $('#fYearMax').value = $('#fPriceMin').value = $('#fPriceMax').value =
    $('#fCondition').value = $('#fSource').value = '';
    $('#fSort').value = 'date_found:desc';
    $$('.rating-cb').forEach(cb => cb.checked = false);
    $$('.chip-btn').forEach(b => b.classList.remove('active'));
    year2007Only = false;
    fetchListings();
  });

  // Apply filters button (for mobile)
  $('#btnApply').addEventListener('click', fetchListings);

  // 2007-only chip
  $$('.chip-btn[data-year]').forEach(btn => {
    btn.addEventListener('click', () => {
      const y = btn.dataset.year;
      year2007Only = !year2007Only;
      btn.classList.toggle('active', year2007Only);
      $('#fYearMin').value = year2007Only ? y : '';
      $('#fYearMax').value = year2007Only ? y : '';
      fetchListings();
    });
  });

  setupAutoFilter();
});

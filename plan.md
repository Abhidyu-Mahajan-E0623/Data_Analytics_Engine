# Anomaly Dashboard — UI Changes Plan (Round 2)

> **Scope**: Five UI changes to the anomaly detection results page. All changes in `dashboard/` only.
> **Tables in use**: `snr_dim_snr_change_log`, `snr_dim_snr_demographics`, `snr_dim_snr_product`, `snr_fact_snr_control`, `snr_fact_snr_sales` (all under `new_claim_catalog.bronze`)

---

## Change 1 — Description Text: Remove Italics, Use Normal Color

### Problem
The table description text is currently styled with `font-style: italic` and uses `color: var(--g400)` (a light gray), making it look washed out.

### Current Code
In `dashboard/styles.css` (line 128):
```css
.tbl-description{font-size:.8rem;color:var(--g400);line-height:1.45;margin-top:4px;font-weight:400;font-style:italic;max-width:600px}
```

### Fix
Change `font-style:italic` → `font-style:normal` and change `color:var(--g400)` → `color:var(--g700)`.

```css
.tbl-description{font-size:.8rem;color:var(--g700);line-height:1.45;margin-top:4px;font-weight:400;font-style:normal;max-width:600px}
```

---

## Change 2 — Dynamic Header Title Per Module

### Problem
The header `<h1>` always says "ProcDNA Intelligent Core". It should change to:
- **Home**: `Nexora Analytics Dashboard`
- **Anomaly view**: `Nexora Analytics Dashboard`
- **Hypothesis view**: `Nexora Hypothesis Engine`
- **Insight view**: `Nexora Insight Engine`

### Current Code

**`dashboard/index.html` line 28:**
```html
<h1>ProcDNA Intelligent Core</h1>
```

**`dashboard/app.js` line 34-51 (`navigate` function):**
```js
navigate: function(viewId) {
    ...
    document.getElementById('header-sub').innerText =
        (viewId === 'home') ? 'AI-Powered Data Pipeline Assistant' : 'Diagnostic & Generation Dashboard';
},
```

### Fix

**A) `index.html` line 28** — Change static text + give it an id:
```html
<h1 id="header-title">Nexora Analytics Dashboard</h1>
```

**B) `app.js` `navigate()` function** — After setting `header-sub`, also update the `<h1>`:
```js
// Title map
var titles = {
    'home':       'Nexora Analytics Dashboard',
    'anomaly':    'Nexora Analytics Dashboard',
    'hypothesis': 'Nexora Hypothesis Engine',
    'insight':    'Nexora Insight Engine',
    'loading':    'Nexora Analytics Dashboard'
};
document.getElementById('header-title').innerText = titles[viewId] || 'Nexora Analytics Dashboard';
```

Also update the subtitle map:
```js
var subtitles = {
    'home':       'AI-Powered Data Pipeline Assistant',
    'anomaly':    'Anomaly Detection Module',
    'hypothesis': 'Hypothesis Generation Module',
    'insight':    'Insight Generation Module',
    'loading':    'Processing Pipeline...'
};
document.getElementById('header-sub').innerText = subtitles[viewId] || 'AI-Powered Data Pipeline Assistant';
```

---

## Change 3 — KPI "Accepted" Stat: Show "X then Tables Accepted"

### Problem
The third stat box in the KPI strip says just a number with label "Accepted". The user wants the label to read **"Tables Accepted"** and the number should remain above it.

### Current Code
**`dashboard/index.html` line 102:**
```html
<div class="stat-box stat-ok"><span class="stat-val" id="tables-passed">0</span><span class="stat-lbl">Accepted</span></div>
```

### Fix
Change the label text from `Accepted` to `Tables Accepted`:
```html
<div class="stat-box stat-ok"><span class="stat-val" id="tables-passed">0</span><span class="stat-lbl">Tables Accepted</span></div>
```

---

## Change 4 — Add Datasource & Table Filters + Friendly Month Names

### Problem
- There is no Datasource filter (should allow: All, SNR).
- There is no Table filter (should show real table names from the report).
- The period dropdown shows `2025-10` instead of `October 2025`.

### Current Code

**`dashboard/index.html` lines 92-97 (filter bar):**
```html
<div class="period-selector-bar">
    <label for="period-select">📅 Filter by Period:</label>
    <select id="period-select" onchange="...">
        <option value="all">All Periods</option>
    </select>
</div>
```

**`dashboard/app.js` state (line 21-29):**
```js
state: { ..., filterMonth: 'all' },
```

**`dashboard/app.js` renderer (lines 369-376):**
```js
// Populate period selector
var sel = document.getElementById('period-select');
(data.availableMonths || []).forEach(function(m) {
    sel.innerHTML += '<option value="' + m + '">' + m + '</option>';
});
```

### Fix

**A) Add state properties in `app.js`:**
```js
state: {
    ...,
    filterMonth: 'all',
    filterDatasource: 'all',
    filterTable: 'all'
},
```

**B) Add a month-name formatter function in `app.js`:**
```js
formatMonth: function(ym) {
    // "2025-10" → "October 2025"
    var months = ['January','February','March','April','May','June',
                  'July','August','September','October','November','December'];
    var parts = ym.split('-');
    var mIdx = parseInt(parts[1], 10) - 1;
    return months[mIdx] + ' ' + parts[0];
},
```

**C) Replace the single `<select>` in `index.html` with three inline filters:**
```html
<div class="period-selector-bar">
    <label>🗄 Datasource:</label>
    <select id="filter-datasource" onchange="app.state.filterDatasource=this.value;app.applyFilters();">
        <option value="all">All</option>
        <option value="snr">SNR</option>
    </select>

    <label>📋 Table:</label>
    <select id="filter-table" onchange="app.state.filterTable=this.value;app.applyFilters();">
        <option value="all">All Tables</option>
    </select>

    <label>📅 Time Period:</label>
    <select id="period-select" onchange="app.state.filterMonth=this.value;app.applyFilters();">
        <option value="all">All Periods</option>
    </select>
</div>
```

**D) Add `applyFilters()` helper in `app.js`:**
```js
applyFilters: function() {
    if (this.state.anomalyData) this.renderers.anomaly(this.state.anomalyData);
},
```

**E) Update `renderers.anomaly()` in `app.js`:**

1. **Populate Table filter** — extract unique table short-names from `data.tables`, populate `#filter-table`:
   ```js
   var tblSel = document.getElementById('filter-table');
   var curTbl = app.state.filterTable || 'all';
   tblSel.innerHTML = '<option value="all">All Tables</option>';
   data.tables.forEach(function(tbl) {
       var parts = tbl.tableName.split('.');
       var short = parts[parts.length - 1]; // e.g. "snr_fact_snr_sales"
       tblSel.innerHTML += '<option value="' + short + '"' + (curTbl === short ? ' selected' : '') + '>' + short + '</option>';
   });
   ```

2. **Populate Period filter with friendly names**:
   ```js
   var sel = document.getElementById('period-select');
   var curVal = app.state.filterMonth || 'all';
   sel.innerHTML = '<option value="all">All Periods</option>';
   (data.availableMonths || []).forEach(function(m) {
       sel.innerHTML += '<option value="' + m + '"' + (curVal === m ? ' selected' : '') + '>' + app.formatMonth(m) + '</option>';
   });
   ```

3. **Filter tables by datasource + table name** before the `forEach` rendering loop:
   ```js
   var filteredTables = data.tables.filter(function(tbl) {
       var parts = tbl.tableName.split('.');
       var short = parts[parts.length - 1];
       // Datasource filter
       if (app.state.filterDatasource !== 'all') {
           if (app.state.filterDatasource === 'snr' && short.indexOf('snr') !== 0) return false;
       }
       // Table filter
       if (app.state.filterTable !== 'all' && short !== app.state.filterTable) return false;
       return true;
   });
   // Use filteredTables instead of data.tables for the rendering loop
   ```

---

## Change 5 — Show Only Monthly Data (Remove Weekly/Daily)

### Problem
The renderer currently shows 3 sub-sections per card (Monthly, Weekly, Daily). The user only wants **Monthly** for now.

### Current Code
**`dashboard/app.js` renderer (inside `renderers.anomaly()`):**
```js
var granularities = [
    { key: 'monthlyRows', label: '📅 Monthly' },
    { key: 'weeklyRows', label: '📅 Weekly' },
    { key: 'dailyRows', label: '📅 Daily' }
];
```

### Fix
Remove the Weekly and Daily entries, keep only Monthly. Also remove the `<h4>` sub-heading since there's only one section now:
```js
var granularities = [
    { key: 'monthlyRows', label: '' }
];
```

And in the rendering loop, skip the `<h4>` label since there's only one granularity:
```js
// Remove this line from the loop:
// bodyHtml += '<h4 class="granularity-label">' + g.label + '</h4>';
```

The parser should still parse all granularities (no change to parser) for future use, but the renderer only shows monthly.

---

## File Change Summary

| File | Changes |
|------|---------|
| `dashboard/styles.css` | Change `.tbl-description` to `font-style:normal` + `color:var(--g700)` |
| `dashboard/index.html` | Rename `<h1>` to "Nexora Analytics Dashboard" + add `id="header-title"`. Change KPI label to "Tables Accepted". Replace single period `<select>` with 3 filters (Datasource, Table, Time Period). |
| `dashboard/app.js` | Add `filterDatasource`/`filterTable` to state. Add `formatMonth()` + `applyFilters()`. Update `navigate()` with title/subtitle maps. Update renderer: populate 3 filter dropdowns, filter tables, show friendly month names, render only monthly rows. |

---

## Verification Plan

1. Run FastAPI server: `.venv\Scripts\python.exe -m uvicorn api.server:app --host 0.0.0.0 --port 8050`
2. Open `http://localhost:8050`, hard refresh (Ctrl+F5).
3. **Change 1**: Verify description text below table names is not italic and uses a readable dark color.
4. **Change 2**: Click Anomaly → header says "Nexora Analytics Dashboard". Click Hypothesis → "Nexora Hypothesis Engine". Click Insight → "Nexora Insight Engine". Go Home → "Nexora Analytics Dashboard".
5. **Change 3**: Verify the third KPI box reads "{N} Tables Accepted".
6. **Change 4**: Verify 3 filter dropdowns (Datasource, Table, Time Period). Select "SNR" datasource → all tables still show. Select a specific table → only that table's card shows. Select a period → shows friendly name like "October 2025" and filters rows correctly.
7. **Change 5**: Verify only monthly rows appear (no Weekly/Daily sub-sections or labels).

/* ==========================================================================
   ProcDNA Intelligent Core — Application Controller
   ========================================================================== */

var app = {

    /* Human-readable anomaly descriptions per table (Fix 3) */
    TABLE_DESCRIPTIONS: {
        'snr_dim_snr_change_log':
            'Monitors the volume of data change records. A spike could mean a bulk data load or system error; a dip could mean missing data ingestion.',
        'snr_dim_snr_demographics':
            'Tracks outlet records by trade class (e.g., Hospitals, Retail Pharmacies). Unusual changes may indicate data quality issues or market shifts.',
        'snr_dim_snr_product':
            'Watches product catalog additions per period. Abnormal counts may signal catalog errors, missed uploads, or bulk imports.',
        'snr_fact_snr_control':
            'Checks control-group volume units (a quality benchmark). Values outside the normal range may indicate supply chain disruptions or data pipeline errors.',
        'snr_fact_snr_sales':
            'Tracks total sales volume units over time. Flagged entries mean the sales volume is unusually high or low compared to the historical pattern.'
    },

    state: {
        currentView: 'home',
        anomalyData: null,
        hypothesisData: null,
        insightData: null,
        acceptedTables: {},
        abortController: null,
        filterMonth: 'all',
        filterDatasource: 'all',
        filterTable: 'all'
    },

    /* ------------------------------------------------------------------
       Navigation
       ------------------------------------------------------------------ */
    navigate: function(viewId) {
        var views = document.querySelectorAll('.view');
        for (var i = 0; i < views.length; i++) {
            views[i].classList.remove('visible');
            views[i].style.display = 'none';
        }
        var target = document.getElementById('view-' + viewId);
        if (!target) return;
        target.style.display = 'block';
        void target.offsetWidth;
        target.classList.add('visible');
        window.scrollTo({ top: 0, behavior: 'smooth' });

        this.state.currentView = viewId;
        var nav = document.getElementById('header-nav');
        nav.style.display = (viewId === 'home') ? 'none' : 'flex';
        
        var titles = {
            'home':       'Nexora Analytics Dashboard',
            'anomaly':    'Nexora Analytics Dashboard',
            'hypothesis': 'Nexora Hypothesis Engine',
            'insight':    'Nexora Insight Engine',
            'loading':    'Nexora Analytics Dashboard'
        };
        document.getElementById('header-title').innerText = titles[viewId] || 'Nexora Analytics Dashboard';

        var subtitles = {
            'home':       'AI-Powered Data Pipeline Assistant',
            'anomaly':    'Anomaly Detection Module',
            'hypothesis': 'Hypothesis Generation Module',
            'insight':    'Insight Generation Module',
            'loading':    'Processing Pipeline...'
        };
        document.getElementById('header-sub').innerText = subtitles[viewId] || 'AI-Powered Data Pipeline Assistant';
    },

    goHome: function() {
        // Cancel any in-flight request
        if (this.state.abortController) {
            this.state.abortController.abort();
            this.state.abortController = null;
        }
        this.closeModals();
        this.navigate('home');
    },

    /* ------------------------------------------------------------------
       Modals
       ------------------------------------------------------------------ */
    showHypothesisModal: function() {
        document.getElementById('modal-hypothesis').style.display = 'flex';
    },
    showInsightModal: function() {
        document.getElementById('modal-insight').style.display = 'flex';
    },
    closeModals: function() {
        document.getElementById('modal-hypothesis').style.display = 'none';
        document.getElementById('modal-insight').style.display = 'none';
    },

    /* ------------------------------------------------------------------
       Process-specific log steps (based on real pipeline stages)
       ------------------------------------------------------------------ */
    logSteps: {
        anomaly: [
            'Loading configuration & credentials',
            'Connecting to Databricks cluster',
            'Fetching SNR tables from Bronze schema',
            'Computing quartile boundaries (Jan–Oct 2025)',
            'Scanning November data for statistical outliers',
            'Aggregating results per table',
            'Generating anomaly report file'
        ],
        hypothesis: [
            'Loading configuration & credentials',
            'Connecting to Databricks SQL endpoint',
            'Fetching Unity Catalog metadata for SNR tables',
            'Selecting top-K context bundle',
            'Building LLM generation prompt',
            'Calling Azure OpenAI for hypothesis generation',
            'Validating & auto-repairing hypotheses',
            'Creating metrics tables in Databricks',
            'Saving hypothesis artifacts'
        ],
        insight: [
            'Loading configuration & credentials',
            'Resolving hypothesis run ID',
            'Loading hypotheses & metrics tables',
            'Evaluating metric calculations per hypothesis',
            'Calling Azure OpenAI for insight synthesis',
            'Formatting business-ready insight report',
            'Saving insight output file'
        ]
    },

    /* ------------------------------------------------------------------
       Load Report — runs pipeline then fetches report
       ------------------------------------------------------------------ */
    loadReport: function(type, extraBody) {
        var self = this;
        self.closeModals();
        self.navigate('loading');

        var titles = {
            anomaly: 'Running Anomaly Detection Pipeline',
            hypothesis: 'Running Hypothesis Generation Pipeline',
            insight: 'Running Insight Generation Pipeline'
        };
        document.getElementById('loading-title').innerText = titles[type] + '...';
        document.getElementById('loading-subtitle').innerText = 'Connecting to Databricks cluster.';

        // Build step log UI
        var logContainer = document.getElementById('step-log');
        logContainer.innerHTML = '';
        var steps = self.logSteps[type] || [];
        var stepEls = [];
        for (var s = 0; s < steps.length; s++) {
            var div = document.createElement('div');
            div.className = 'step-item';
            div.innerHTML = '<div class="step-dot"></div><span>' + steps[s] + '</span>';
            logContainer.appendChild(div);
            stepEls.push(div);
        }

        // Animate steps one by one
        var stepIdx = 0;
        var stepTimer = setInterval(function() {
            if (stepIdx > 0 && stepIdx - 1 < stepEls.length) {
                stepEls[stepIdx - 1].classList.remove('active');
                stepEls[stepIdx - 1].classList.add('done');
            }
            if (stepIdx < stepEls.length) {
                stepEls[stepIdx].classList.add('active');
                document.getElementById('loading-subtitle').innerText = steps[stepIdx] + '...';
                stepIdx++;
            }
        }, 1800);

        // Build POST body
        var body = extraBody || {};
        if (type === 'anomaly' && !body.schema) body = { schema: 'bronze' };

        // AbortController for cancel-on-home
        var controller = new AbortController();
        self.state.abortController = controller;

        fetch('/api/' + type, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
            signal: controller.signal
        })
        .then(function(res) {
            clearInterval(stepTimer);
            // Mark all steps done
            for (var i = 0; i < stepEls.length; i++) {
                stepEls[i].classList.remove('active');
                stepEls[i].classList.add('done');
            }
            if (!res.ok) throw new Error('Pipeline returned HTTP ' + res.status);
            return res.json();
        })
        .then(function() {
            document.getElementById('loading-title').innerText = 'Pipeline Complete!';
            document.getElementById('loading-subtitle').innerText = 'Fetching generated report...';
            return fetch('/api/latest_' + type, { signal: controller.signal });
        })
        .then(function(res) {
            if (!res.ok) throw new Error('Report not found. HTTP ' + res.status);
            return res.text();
        })
        .then(function(text) {
            self.state.abortController = null;
            if (type === 'anomaly') {
                self.state.anomalyData = self.parsers.anomaly(text);
                self.renderers.anomaly(self.state.anomalyData);
                self.navigate('anomaly');
            } else if (type === 'hypothesis') {
                self.state.hypothesisData = self.parsers.hypothesis(text);
                self.renderers.hypothesis(self.state.hypothesisData);
                self.navigate('hypothesis');
            } else if (type === 'insight') {
                self.state.insightData = self.parsers.insight(text);
                self.renderers.insight(self.state.insightData);
                self.navigate('insight');
            }
        })
        .catch(function(err) {
            clearInterval(stepTimer);
            self.state.abortController = null;
            if (err.name === 'AbortError') return; // user cancelled
            console.error(err);
            alert('Pipeline error: ' + err.message);
            self.navigate('home');
        });
    },

    /* ------------------------------------------------------------------
       Modal action handlers
       ------------------------------------------------------------------ */
    runHypothesis: function() {
        var schema = document.getElementById('hyp-schema').value;
        var domain = document.getElementById('hyp-domain').value.trim();
        if (!domain) { alert('Please enter at least one domain.'); return; }
        this.loadReport('hypothesis', { schema: schema, domain: domain });
    },

    runInsight: function() {
        var runId = document.getElementById('ins-runid').value.trim();
        var hidsRaw = document.getElementById('ins-hids').value.trim();
        var hids = [];
        if (hidsRaw) {
            hids = hidsRaw.split(',').map(function(s) { return parseInt(s.trim()); }).filter(function(n) { return !isNaN(n); });
        }
        var body = {};
        if (runId) body.run_id = runId;
        if (hids.length > 0) body.hypothesis_ids = hids;
        this.loadReport('insight', body);
    },

    /* ==================================================================
       PARSERS
       ================================================================== */
    parsers: {
        anomaly: function(text) {
            var lines = text.split('\n');
            var report = { checksRun: 0, issuesFound: 0, tables: [], availableMonths: [] };
            for (var i = 0; i < lines.length; i++) {
                var t = lines[i].trim();
                if (t.indexOf('Checks Run') === 0) report.checksRun = parseInt(t.split(':')[1]) || 0;
                if (t.indexOf('Issues Found') === 0) report.issuesFound = parseInt(t.split(':')[1]) || 0;
            }
            var sections = [], cur = null;
            for (var i = 0; i < lines.length; i++) {
                var t = lines[i].trim();
                if (t.indexOf('CHECK:') === 0) { if (cur) sections.push(cur); cur = { lines: [t] }; }
                else if (cur) cur.lines.push(lines[i]);
            }
            if (cur) sections.push(cur);

            report.tables = sections.map(function(sec) {
                var tbl = { tableName: '', status: 'ok', anomalyCount: 0, monthlyRows: [], weeklyRows: [], dailyRows: [], rows: [] };
                for (var j = 0; j < sec.lines.length; j++) {
                    var t = sec.lines[j].trim();
                    if (t.indexOf('Table:') === 0) tbl.tableName = t.replace('Table:', '').trim();
                    if (t.indexOf('Result:') === 0) {
                        var m = t.match(/Result:\s*(.+?)\s*\((\d+)\s*finding/);
                        if (m) { tbl.status = m[1].trim() === 'Issues Detected' ? 'anomaly' : (m[1].trim() === 'Skipped' ? 'skipped' : 'ok'); tbl.anomalyCount = parseInt(m[2]) || 0; }
                    }
                }
                // Track current granularity while parsing rows
                var currentGranularity = 'monthly';
                var inData = false;
                for (var j = 0; j < sec.lines.length; j++) {
                    var t = sec.lines[j].trim();
                    // Detect granularity switches
                    if (t.indexOf('MONTHLY') >= 0) { currentGranularity = 'monthly'; inData = false; }
                    else if (t.indexOf('WEEKLY') >= 0) { currentGranularity = 'weekly'; inData = false; }
                    else if (t.indexOf('DAILY') >= 0) { currentGranularity = 'daily'; inData = false; }
                    if (/^-{10,}$/.test(t)) { inData = true; continue; }
                    if (inData && t && t.indexOf('Month') !== 0 && t.indexOf('Week') !== 0 && t.indexOf('Date') !== 0 && t.indexOf('Group') !== 0 && t.indexOf('---') !== 0) {
                        var sm = t.match(/(Unusually High|Unusually Low|OK)\s*/);
                        if (sm) {
                            var dir = sm[1]; var rem = t.substring(0, sm.index).trim();
                            var rm = rem.match(/([\d,.\-]+)\s*-\s*([\d,.\-]+)$/);
                            if (rm) { var rng = rm[0]; rem = rem.substring(0, rm.index).trim();
                                var pts = rem.split(/\s+/);
                                if (pts.length >= 2) { var val = pts.pop(), per = pts.shift(), grp = pts.length > 0 ? pts.join(' ') : null;
                                    var row = { period: per, group: grp === 'ALL' ? null : grp, value: val, range: rng, direction: dir };
                                    tbl[currentGranularity + 'Rows'].push(row);
                                    tbl.rows.push(row);
                                }
                            }
                        }
                    }
                }
                return tbl;
            });

            // Extract unique YYYY-MM months from all rows
            var allPeriods = [];
            report.tables.forEach(function(tbl) {
                var all = tbl.monthlyRows.concat(tbl.weeklyRows).concat(tbl.dailyRows);
                all.forEach(function(r) {
                    var ym = r.period.substring(0, 7);
                    if (allPeriods.indexOf(ym) < 0) allPeriods.push(ym);
                });
            });
            allPeriods.sort();
            report.availableMonths = allPeriods;

            return report;
        },

        hypothesis: function(text) {
            var blocks = text.split(/(?=H\d+\s*\[[^\]]*\]\s*\n)/);
            var out = [];
            for (var b = 0; b < blocks.length; b++) {
                if (!blocks[b].trim().match(/^H\d/)) continue;
                var h = { id: '', statement: '', tables: '', cols: [], notes: '' };
                var lines = blocks[b].split('\n'); h.id = lines[0].trim();
                var inCols = false;
                for (var i = 1; i < lines.length; i++) {
                    var t = lines[i].trim();
                    if (t.indexOf('Statement') === 0) { h.statement = t.replace(/^Statement\s*:/, '').trim(); inCols = false; }
                    else if (t.indexOf('Tables:') === 0) { h.tables = t.replace(/^Tables:/, '').trim(); inCols = false; }
                    else if (t.indexOf('Notes') === 0) { h.notes = t.replace(/^Notes\s*:/, '').trim(); inCols = false; }
                    else if (t.indexOf('Required cols') === 0) inCols = true;
                    else if (inCols && t.indexOf('-') === 0) h.cols.push(t.substring(1).trim());
                    else if (inCols && t && !t.match(/^-/)) inCols = false;
                }
                out.push(h);
            }
            return out;
        },

        insight: function(text) {
            var blocks = text.split(/(?=INSIGHT\s*\d+:)/); var out = [];
            for (var b = 0; b < blocks.length; b++) {
                if (blocks[b].trim().indexOf('INSIGHT') !== 0) continue;
                var lines = blocks[b].split('\n');
                var ins = { title: '', hypothesis: '', output: '', reasoning: '', metrics: [] };
                ins.title = lines[0].replace(/-*/g, '').trim();
                var sec = '';
                for (var i = 1; i < lines.length; i++) {
                    var t = lines[i].trim();
                    if (t.indexOf('Hypothesis Used:') === 0) { sec = 'h'; continue; }
                    if (t.indexOf('Template Output:') === 0) { sec = 'o'; continue; }
                    if (t.indexOf('Reasoning') === 0 && t.indexOf('Calculation') > 0) { sec = 'r'; continue; }
                    if (t.indexOf('Key Metrics:') === 0) { sec = 'm'; continue; }
                    if (!t || t.indexOf('===') === 0) { if (t.indexOf('===') === 0) break; continue; }
                    if (sec === 'h') ins.hypothesis += t + ' ';
                    if (sec === 'o') ins.output += t + ' ';
                    if (sec === 'r') ins.reasoning += t + ' ';
                    if (sec === 'm') { var kv = t.split(':'); if (kv.length >= 2) ins.metrics.push({ key: kv[0].trim(), val: kv.slice(1).join(':').trim() }); }
                }
                out.push(ins);
            }
            return out;
        }
    },

    /* ==================================================================
       RENDERERS
       ================================================================== */
    renderers: {
        anomaly: function(data) {
            document.getElementById('total-checks').innerText = data.checksRun;
            document.getElementById('total-issues').innerText = data.issuesFound;
            document.getElementById('tables-passed').innerText = Object.keys(app.state.acceptedTables).length;

            // Populate Table selector
            var tblSel = document.getElementById('filter-table');
            var curTbl = app.state.filterTable || 'all';
            tblSel.innerHTML = '<option value="all">All Tables</option>';
            data.tables.forEach(function(tbl) {
                var parts = tbl.tableName.split('.');
                var short = parts[parts.length - 1];
                tblSel.innerHTML += '<option value="' + short + '"' + (curTbl === short ? ' selected' : '') + '>' + short + '</option>';
            });

            // Populate period selector
            var sel = document.getElementById('period-select');
            var curVal = app.state.filterMonth || 'all';
            sel.innerHTML = '<option value="all">All Periods</option>';
            (data.availableMonths || []).forEach(function(m) {
                sel.innerHTML += '<option value="' + m + '"' + (curVal === m ? ' selected' : '') + '>' + app.formatMonth(m) + '</option>';
            });
            if (curVal !== 'all') sel.value = curVal;

            // Apply Filters (Datasource + Table)
            var filteredTables = data.tables.filter(function(tbl) {
                var parts = tbl.tableName.split('.');
                var short = parts[parts.length - 1];
                if (app.state.filterDatasource !== 'all') {
                    if (app.state.filterDatasource === 'snr' && short.indexOf('snr') !== 0) return false;
                }
                if (app.state.filterTable !== 'all' && short !== app.state.filterTable) return false;
                return true;
            });

            var c = document.getElementById('tables-container'); c.innerHTML = '';
            var globalIssuesCount = 0;

            filteredTables.forEach(function(tbl, idx) {
                var selectedMonth = app.state.filterMonth || 'all';
                var rows = tbl.monthlyRows || [];
                if (selectedMonth !== 'all') {
                    rows = rows.filter(function(r) {
                        return r.period.substring(0, 7) === selectedMonth;
                    });
                }
                
                var localIssuesCount = 0;
                rows.forEach(function(r) {
                    if (r.direction !== 'OK') localIssuesCount++;
                });
                globalIssuesCount += localIssuesCount;

                var isAccepted = !!app.state.acceptedTables[tbl.tableName];
                var card = document.createElement('div');
                card.className = 'table-card' + (isAccepted ? ' accepted' : '');
                card.style.animationDelay = (idx * 80) + 'ms';
                
                var dynStatus = tbl.status;
                if (tbl.status !== 'skipped') {
                    dynStatus = (localIssuesCount > 0) ? 'anomaly' : 'ok';
                }
                var isAnomaly = dynStatus === 'anomaly';
                var badgeTxt = isAnomaly ? localIssuesCount + (localIssuesCount === 1 ? ' Issue' : ' Issues') : dynStatus.toUpperCase();
                var badge = '<span class="status-badge ' + dynStatus + '">' + badgeTxt + '</span>';

                var desc = app.getTableDescription(tbl.tableName);
                var hdr = document.createElement('div'); hdr.className = 'tbl-hdr';
                var info = '<div><div class="tbl-name">Table: ' + app.esc(tbl.tableName) + '</div>'
                    + (desc ? '<p class="tbl-description">' + app.esc(desc) + '</p>' : '')
                    + '<div style="margin-top:4px">' + badge + '</div></div>';
                
                var btns = '<div class="tbl-actions">';
                if (isAccepted) {
                    btns += '<span class="accepted-label">✓ Accepted</span>';
                } else {
                    btns += '<button class="btn btn-accept" onclick="app.acceptTable(\'' + app.esc(tbl.tableName).replace(/'/g, "\\'") + '\')">✓ Accept</button>';
                }
                if (isAnomaly && !isAccepted) btns += '<button class="btn btn-review" onclick="window.open(\'https://portal.azure.com/#@Procdnaanalytics.onmicrosoft.com/resource/subscriptions/2b6cd5ce-1909-463b-8f97-eb2d0447c776/resourceGroups/PROCDNA-GENAI-RG/providers/Microsoft.Storage/storageAccounts/dataanalyticsengine/storagebrowser\',\'_blank\')">✗ Review</button>';
                btns += '</div>';
                hdr.innerHTML = info + btns; card.appendChild(hdr);

                var hasAnyRows = rows.length > 0;
                if (hasAnyRows) {
                    var bodyHtml = '';
                    var hasGroup = rows.some(function(r) { return r.group !== null; });
                    bodyHtml += '<table class="data-table"><thead><tr><th>Period</th>' + (hasGroup ? '<th>Group</th>' : '') + '<th class="col-r">Value</th><th class="col-r">Expected Range</th><th class="col-r">Status</th></tr></thead><tbody>';
                    rows.forEach(function(r) {
                        bodyHtml += '<tr><td>' + app.esc(app.formatMonth(r.period.substring(0,7))) + '</td>' + (hasGroup ? '<td>' + app.esc(r.group || '') + '</td>' : '') + '<td class="col-r">' + app.esc(r.value) + '</td><td class="col-r">' + app.esc(r.range) + '</td><td class="col-r ' + (r.direction.indexOf('High') >= 0 ? 'dir-hi' : 'dir-lo') + '">' + app.esc(r.direction) + '</td></tr>';
                    });
                    bodyHtml += '</tbody></table>';
                    var body = document.createElement('div'); body.className = 'tbl-body';
                    body.innerHTML = bodyHtml;
                    card.appendChild(body);
                }

                c.appendChild(card);
            });
            document.getElementById('total-issues').innerText = globalIssuesCount;
        },

        hypothesis: function(data) {
            document.getElementById('hyp-count').innerText = data.length;
            var c = document.getElementById('hypothesis-container'); c.innerHTML = '';
            data.forEach(function(h) {
                var card = document.createElement('div'); card.className = 'content-card';
                card.innerHTML = '<div class="card-hdr"><span class="card-id">' + app.esc(h.id) + '</span></div>' +
                    '<div class="hyp-stmt">' + app.esc(h.statement) + '</div>' +
                    '<div class="hyp-meta"><div class="meta-blk"><h4>Target Table</h4><p>' + app.esc(h.tables) + '</p></div><div class="meta-blk"><h4>Required Columns</h4><p>' + app.esc(h.cols.join(', ')) + '</p></div></div>' +
                    (h.notes ? '<div class="hyp-note"><p><strong>Note:</strong> ' + app.esc(h.notes) + '</p></div>' : '');
                c.appendChild(card);
            });
        },

        insight: function(data) {
            var c = document.getElementById('insight-container'); c.innerHTML = '';
            data.forEach(function(ins) {
                var card = document.createElement('div'); card.className = 'content-card';
                var mHtml = ins.metrics.map(function(m) { return '<div class="metric-pill"><span>' + app.esc(m.key) + ':</span> ' + app.esc(m.val) + '</div>'; }).join('');
                card.innerHTML = '<div class="ins-title">' + app.esc(ins.title) + '</div>' +
                    '<div class="ins-body"><p><strong>Core Finding:</strong> ' + app.esc(ins.output) + '</p><p><strong>Reasoning:</strong> ' + app.esc(ins.reasoning) + '</p><p><strong>Hypothesis Used:</strong> <em>' + app.esc(ins.hypothesis) + '</em></p></div>' +
                    (mHtml ? '<div class="metrics-row">' + mHtml + '</div>' : '');
                c.appendChild(card);
            });
        }
    },

    /* ------------------------------------------------------------------
       Actions & Helpers
       ------------------------------------------------------------------ */
    applyFilters: function() {
        if (this.state.anomalyData) this.renderers.anomaly(this.state.anomalyData);
    },

    formatMonth: function(ym) {
        if (!ym || ym.length < 7) return ym;
        var months = ['January','February','March','April','May','June',
                      'July','August','September','October','November','December'];
        var parts = ym.split('-');
        var mIdx = parseInt(parts[1], 10) - 1;
        if (isNaN(mIdx) || mIdx < 0 || mIdx > 11) return ym;
        return months[mIdx] + ' ' + parts[0];
    },

    acceptTable: function(name) {
        this.state.acceptedTables[name] = true;
        this.showAcceptOverlay(name);
        if (this.state.anomalyData) this.renderers.anomaly(this.state.anomalyData);
    },

    /* Fix 2: Beautiful accept overlay */
    showAcceptOverlay: function(tableName) {
        var overlay = document.getElementById('accept-overlay');
        var msg = document.getElementById('accept-overlay-msg');
        msg.innerText = '"' + tableName + '" has been successfully accepted into the pipeline.';
        overlay.style.display = 'flex';
        // Force reflow so animations replay
        var tile = overlay.querySelector('.accept-tile');
        tile.style.animation = 'none'; void tile.offsetWidth; tile.style.animation = '';
        overlay.style.animation = 'none'; void overlay.offsetWidth; overlay.style.animation = '';
        // Reset SVG animations
        var circle = overlay.querySelector('.check-circle');
        var path = overlay.querySelector('.check-path');
        if (circle) { circle.style.animation = 'none'; void circle.offsetWidth; circle.style.animation = ''; }
        if (path) { path.style.animation = 'none'; void path.offsetWidth; path.style.animation = ''; }
        // Reset sparkles
        var sparkles = overlay.querySelectorAll('.sparkle');
        for (var i = 0; i < sparkles.length; i++) {
            sparkles[i].style.animation = 'none'; void sparkles[i].offsetWidth; sparkles[i].style.animation = '';
        }
        // Auto-dismiss after 3.5 seconds
        var self = this;
        if (this._overlayTimer) clearTimeout(this._overlayTimer);
        this._overlayTimer = setTimeout(function() { self.closeAcceptOverlay(); }, 3500);
    },

    closeAcceptOverlay: function() {
        var overlay = document.getElementById('accept-overlay');
        if (!overlay || overlay.style.display === 'none') return;
        var tile = overlay.querySelector('.accept-tile');
        tile.style.animation = 'tileExit 0.4s ease forwards';
        overlay.style.animation = 'overlayFadeOut 0.4s ease forwards';
        setTimeout(function() {
            overlay.style.display = 'none';
            overlay.style.animation = '';
            tile.style.animation = '';
        }, 450);
    },

    /* Fix 3: Get description for a table */
    getTableDescription: function(fullTableName) {
        if (!fullTableName) return '';
        var parts = fullTableName.split('.');
        var shortName = parts[parts.length - 1];
        return this.TABLE_DESCRIPTIONS[shortName] || '';
    },

    showToast: function(msg) {
        var t = document.getElementById('toast');
        document.getElementById('toast-msg').innerText = msg;
        t.style.display = 'flex';
        void t.offsetWidth;
        t.classList.add('show');
        setTimeout(function() { t.classList.remove('show'); setTimeout(function() { t.style.display = 'none'; }, 300); }, 3000);
    },

    esc: function(s) { if (!s) return ''; return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;'); }
};

window.app = app;

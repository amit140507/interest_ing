/**
 * FD comparison: rate view (general/senior/both), bank & tenor filters,
 * best-rate highlight, CSV export, mobile cards.
 */
(function () {
  const DATA_EL = document.getElementById("comparison-data");
  const ROOT = document.getElementById("comparison-root");
  if (!DATA_EL || !ROOT) return;

  let data = {};
  try {
    data = JSON.parse(DATA_EL.textContent || "{}");
  } catch (e) {
    console.error(e);
    return;
  }

  const banks = data.banks || [];
  const rows = data.rows || [];

  const state = {
    rateView: "general",
    tenorPreset: "all",
    bankIds: new Set(banks.map((b) => String(b.id))),
    sortTenorKey: null,
    sortDir: "desc",
  };

  function rowMatchesTenor(row, preset) {
    const mn = row.min_days;
    const mx = row.max_days;
    switch (preset) {
      case "all":
        return true;
      case "under1y":
        return mx <= 364;
      case "y1to3":
        return !(mx < 365 || mn > 1095);
      case "y3to10":
        return !(mx < 1096 || mn > 3650);
      case "y5plus":
        return mn >= 1826;
      default:
        return true;
    }
  }

  function visibleRows() {
    return rows.filter((r) => rowMatchesTenor(r, state.tenorPreset));
  }

  function visibleBanks() {
    return banks.filter((b) => state.bankIds.has(String(b.id)));
  }

  function tenorCellValue(row, bankId) {
    const c = row.by_bank_id[String(bankId)] || {};
    const g = c.general == null || Number.isNaN(Number(c.general)) ? null : Number(c.general);
    const s = c.senior == null || Number.isNaN(Number(c.senior)) ? null : Number(c.senior);
    if (state.rateView === "general") return g;
    if (state.rateView === "senior") return s;
    if (g == null && s == null) return null;
    if (g == null) return s;
    if (s == null) return g;
    return Math.max(g, s);
  }

  function visibleBanksSorted(vr) {
    const vb = visibleBanks();
    if (!state.sortTenorKey) return vb;
    const row = vr.find((r) => `${r.min_days}-${r.max_days}` === state.sortTenorKey);
    if (!row) return vb;
    const dir = state.sortDir === "asc" ? 1 : -1;
    return [...vb].sort((a, b) => {
      const av = tenorCellValue(row, a.id);
      const bv = tenorCellValue(row, b.id);
      if (av == null && bv == null) return String(a.display_name).localeCompare(String(b.display_name));
      if (av == null) return 1;
      if (bv == null) return -1;
      if (av !== bv) return (av - bv) * dir;
      return String(a.display_name).localeCompare(String(b.display_name));
    });
  }

  function formatRate(v) {
    if (v == null || v === "") return null;
    const n = Number(v);
    if (Number.isNaN(n)) return null;
    return n.toFixed(2) + "%";
  }

  function formatFetchedAt(iso) {
    if (!iso) return "";
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return String(iso);
    return d.toLocaleString(undefined, {
      dateStyle: "medium",
      timeStyle: "short",
    });
  }

  function bestFlagsForRow(row, vb) {
    const view = state.rateView;
    const genBest = new Set();
    const senBest = new Set();
    if (view === "both") {
      let maxG = -Infinity;
      let maxS = -Infinity;
      vb.forEach((b) => {
        const c = row.by_bank_id[String(b.id)] || {};
        const g = c.general;
        const s = c.senior;
        if (g != null && !Number.isNaN(Number(g))) maxG = Math.max(maxG, Number(g));
        if (s != null && !Number.isNaN(Number(s))) maxS = Math.max(maxS, Number(s));
      });
      vb.forEach((b) => {
        const c = row.by_bank_id[String(b.id)] || {};
        if (c.general != null && Number(c.general) === maxG && maxG > -Infinity) genBest.add(String(b.id));
        if (c.senior != null && Number(c.senior) === maxS && maxS > -Infinity) senBest.add(String(b.id));
      });
    } else {
      let maxV = -Infinity;
      vb.forEach((b) => {
        const c = row.by_bank_id[String(b.id)] || {};
        const v = view === "general" ? c.general : c.senior;
        if (v != null && !Number.isNaN(Number(v))) maxV = Math.max(maxV, Number(v));
      });
      vb.forEach((b) => {
        const c = row.by_bank_id[String(b.id)] || {};
        const v = view === "general" ? c.general : c.senior;
        if (v != null && Number(v) === maxV && maxV > -Infinity) genBest.add(String(b.id));
      });
    }
    return { genBest, senBest };
  }

  function render() {
    const vr = visibleRows();
    const vb = visibleBanksSorted(vr);

    const tableMount = document.getElementById("comparison-table-mount");
    const cardsMount = document.getElementById("comparison-cards-mount");
    if (!tableMount || !cardsMount) return;

    if (!banks.length || !rows.length) {
      tableMount.innerHTML =
        '<p class="empty">No rates in the database yet. Click <strong>Refresh all banks</strong> to fetch.</p>';
      cardsMount.innerHTML = "";
      return;
    }

    if (vr.length === 0) {
      tableMount.innerHTML =
        '<p class="empty">No tenor rows match the current filter. Try <strong>All</strong> or another range.</p>';
      cardsMount.innerHTML = "";
      return;
    }

    const view = state.rateView;
    const both = view === "both";
    const emptyTitle =
      "No exact tenor bucket for this bank on this row (banks use different day ranges).";

    const bestByTenor = new Map();
    vr.forEach((row) => {
      bestByTenor.set(`${row.min_days}-${row.max_days}`, bestFlagsForRow(row, vb));
    });

    let thead = '<thead><tr class="thead-main">';
    thead += '<th scope="col" class="sticky-col corner">Bank</th>';
    vr.forEach((row) => {
      const dl = row.display_label || row.label;
      const sub = row.days_subtitle || "";
      const key = `${row.min_days}-${row.max_days}`;
      const isSorted = state.sortTenorKey === key;
      const sortHint = isSorted ? (state.sortDir === "desc" ? " ↓" : " ↑") : "";
      const sortTitle = isSorted
        ? `Sorted ${state.sortDir === "desc" ? "high to low" : "low to high"}`
        : "Click to sort banks by this time bucket";
      thead += `<th scope="col" class="sticky-top bank-head"><div class="bank-head-inner"><span class="bank-name"><button type="button" class="bank-sort-btn${isSorted ? " active" : ""}" data-sort-tenor-key="${key}" title="${sortTitle}">${escapeHtml(dl)}${sortHint}</button></span>${sub ? `<span class="bank-updated">${escapeHtml(sub)}</span>` : ""}</div></th>`;
    });
    thead += "</tr></thead>";

    let tbody = "<tbody>";
    vb.forEach((b) => {
      const bid = String(b.id);
      const upd = formatFetchedAt(b.fetched_at);
      tbody += "<tr>";
      tbody += `<th scope="row" class="sticky-col tenor"><span class="tenor-title">${escapeHtml(b.display_name)}</span><span class="tenor-days">${upd ? `Updated ${escapeHtml(upd)}` : ""}</span></th>`;
      vr.forEach((row) => {
        const c = row.by_bank_id[bid] || {};
        const flags = bestByTenor.get(`${row.min_days}-${row.max_days}`) || { genBest: new Set(), senBest: new Set() };
        if (both) {
          const g = c.general;
          const s = c.senior;
          const bestG = flags.genBest.has(bid) && g != null;
          const bestS = flags.senBest.has(bid) && s != null;
          const cls = bestG || bestS ? " is-best" : "";
          const txt = `G: ${g == null ? "—" : formatRate(g)} · S: ${s == null ? "—" : formatRate(s)}`;
          tbody += `<td class="rate-cell${cls}" title="${(g == null && s == null) ? emptyTitle : ""}">${txt}</td>`;
        } else {
          const v = view === "general" ? c.general : c.senior;
          const isB = flags.genBest.has(bid) && v != null;
          const cls = isB ? " is-best" : "";
          tbody += `<td class="rate-cell${cls}" title="${v == null ? emptyTitle : ""}">${v == null ? "—" : formatRate(v)}</td>`;
        }
      });
      tbody += "</tr>";
    });
    tbody += "</tbody>";

    tableMount.innerHTML = `
      <div class="scroll-sync-top" aria-hidden="true"><div class="scroll-sync-top-inner"></div></div>
      <div class="scroll-x"><table class="rates">${thead}${tbody}</table></div>
    `;
    wireTopScroller(tableMount);
    wireTenorSortHandlers(tableMount);

    let cards = '<div class="cards-grid">';
    vb.forEach((b) => {
      const upd = formatFetchedAt(b.fetched_at);
      cards += `<article class="bank-card"><h3 class="bank-card-title">${escapeHtml(b.display_name)}`;
      if (upd) cards += `<span class="bank-updated">${escapeHtml(upd)}</span>`;
      cards += "</h3><ul>";
      vr.forEach((row) => {
        const c = row.by_bank_id[String(b.id)] || {};
        const dl = row.display_label || row.label;
        if (both) {
          cards += `<li><span class="card-tenor">${escapeHtml(dl)}</span>`;
          cards += `<span class="card-rates">G: ${c.general == null ? "—" : formatRate(c.general)} · S: ${c.senior == null ? "—" : formatRate(c.senior)}</span></li>`;
        } else {
          const v = view === "general" ? c.general : c.senior;
          cards += `<li><span class="card-tenor">${escapeHtml(dl)}</span><span class="card-rates">${v == null ? "—" : formatRate(v)}</span></li>`;
        }
      });
      cards += "</ul></article>";
    });
    cards += "</div>";
    cardsMount.innerHTML = cards;
  }

  function wireTopScroller(tableMount) {
    const top = tableMount.querySelector(".scroll-sync-top");
    const topInner = tableMount.querySelector(".scroll-sync-top-inner");
    const bottom = tableMount.querySelector(".scroll-x");
    const table = tableMount.querySelector("table.rates");
    if (!top || !topInner || !bottom || !table) return;

    const syncWidths = () => {
      topInner.style.width = `${table.scrollWidth}px`;
      top.style.display = table.scrollWidth > bottom.clientWidth ? "block" : "none";
    };

    let syncing = false;
    top.addEventListener("scroll", () => {
      if (syncing) return;
      syncing = true;
      bottom.scrollLeft = top.scrollLeft;
      syncing = false;
    });
    bottom.addEventListener("scroll", () => {
      if (syncing) return;
      syncing = true;
      top.scrollLeft = bottom.scrollLeft;
      syncing = false;
    });

    syncWidths();
    window.addEventListener("resize", syncWidths);
  }

  function wireTenorSortHandlers(tableMount) {
    tableMount.querySelectorAll("[data-sort-tenor-key]").forEach((btn) => {
      btn.addEventListener("click", () => {
        const key = btn.getAttribute("data-sort-tenor-key");
        if (!key) return;
        if (state.sortTenorKey === key) {
          state.sortDir = state.sortDir === "desc" ? "asc" : "desc";
        } else {
          state.sortTenorKey = key;
          state.sortDir = "desc";
        }
        render();
      });
    });
  }

  function escapeHtml(s) {
    const d = document.createElement("div");
    d.textContent = s;
    return d.innerHTML;
  }

  function exportCsv() {
    const vr = visibleRows();
    const vb = visibleBanksSorted(vr);
    const view = state.rateView;
    const both = view === "both";
    const headers = ["Tenor", "Days range"];
    vb.forEach((b) => {
      if (both) {
        headers.push(b.display_name + " (General)", b.display_name + " (Senior)");
      } else {
        headers.push(b.display_name + (view === "general" ? " (General)" : " (Senior)"));
      }
    });
    const lines = [headers.map(csvEscape).join(",")];
    vr.forEach((row) => {
      const line = [
        row.display_label || row.label,
        `${row.min_days}–${row.max_days} days`,
      ];
      vb.forEach((b) => {
        const c = row.by_bank_id[String(b.id)] || {};
        if (both) {
          line.push(fmtCsvNum(c.general), fmtCsvNum(c.senior));
        } else {
          line.push(fmtCsvNum(view === "general" ? c.general : c.senior));
        }
      });
      lines.push(line.map(csvEscape).join(","));
    });
    const blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = "fd-comparison.csv";
    a.click();
    URL.revokeObjectURL(a.href);
  }

  function fmtCsvNum(v) {
    if (v == null || v === "") return "";
    const n = Number(v);
    return Number.isNaN(n) ? "" : n.toFixed(2);
  }

  function csvEscape(cell) {
    const s = String(cell);
    if (/[",\n]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
    return s;
  }

  document.querySelectorAll("[data-rate-view]").forEach((btn) => {
    btn.addEventListener("click", () => {
      state.rateView = btn.getAttribute("data-rate-view") || "general";
      document.querySelectorAll("[data-rate-view]").forEach((b) => {
        b.classList.toggle("active", b.getAttribute("data-rate-view") === state.rateView);
      });
      render();
    });
  });

  document.querySelectorAll("[data-tenor-preset]").forEach((btn) => {
    btn.addEventListener("click", () => {
      state.tenorPreset = btn.getAttribute("data-tenor-preset") || "all";
      document.querySelectorAll("[data-tenor-preset]").forEach((b) => {
        b.classList.toggle("active", b.getAttribute("data-tenor-preset") === state.tenorPreset);
      });
      render();
    });
  });

  const bankFilterEl = document.getElementById("bank-filter-list");
  if (bankFilterEl && banks.length) {
    bankFilterEl.innerHTML = banks
      .map((b) => {
        const id = `bf-${b.id}`;
        return `<label class="bank-filter-item"><input type="checkbox" checked data-bank-id="${b.id}" id="${id}"> <span>${escapeHtml(b.display_name)}</span></label>`;
      })
      .join("");
    bankFilterEl.querySelectorAll("input[data-bank-id]").forEach((inp) => {
      inp.addEventListener("change", () => {
        state.bankIds.clear();
        bankFilterEl.querySelectorAll("input[data-bank-id]:checked").forEach((c) => {
          state.bankIds.add(String(c.getAttribute("data-bank-id")));
        });
        if (state.bankIds.size === 0) state.bankIds.add(String(banks[0].id));
        render();
      });
    });
  }

  const btnExport = document.getElementById("btn-export-csv");
  if (btnExport) btnExport.addEventListener("click", exportCsv);

  document.querySelectorAll("[data-rate-view]").forEach((b) => {
    b.classList.toggle("active", b.getAttribute("data-rate-view") === state.rateView);
  });
  document.querySelectorAll("[data-tenor-preset]").forEach((b) => {
    b.classList.toggle("active", b.getAttribute("data-tenor-preset") === state.tenorPreset);
  });

  render();
})();

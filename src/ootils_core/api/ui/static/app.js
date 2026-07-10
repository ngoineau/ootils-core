"use strict";

/*
 * app.js — EXP-1 human window client (ADR-036).
 *
 * Vanilla JS, no framework, no build step. This file is the ONLY place the
 * operator's token is held (sessionStorage — never a cookie, never a URL,
 * never logged to the console). Every read below is a plain Bearer call
 * against an EXISTING /v1/* endpoint; this script has no business logic of
 * its own — it renders what the API returns, None/null-honestly ("n/a" is
 * printed only for a genuine null/undefined, never in place of a real 0).
 */

(function () {
  var TOKEN_KEY = "ootils_token";

  var STATUS_MESSAGES = {
    401: "Not authenticated — check your token.",
    403: "Not authorized for this action (missing scope).",
  };

  function qs(id) {
    return document.getElementById(id);
  }

  function fmt(value) {
    if (value === null || value === undefined || value === "") {
      return "n/a";
    }
    return String(value);
  }

  function getToken() {
    return sessionStorage.getItem(TOKEN_KEY) || "";
  }

  function setToken(value) {
    sessionStorage.setItem(TOKEN_KEY, value);
  }

  function clearToken() {
    sessionStorage.removeItem(TOKEN_KEY);
  }

  function buildUrl(path, params) {
    var url = new URL(path, window.location.origin);
    Object.keys(params || {}).forEach(function (key) {
      var value = params[key];
      if (value !== undefined && value !== null && value !== "") {
        url.searchParams.set(key, value);
      }
    });
    return url.pathname + url.search;
  }

  function apiFetch(path) {
    var token = getToken();
    var headers = token ? { Authorization: "Bearer " + token } : {};
    return fetch(path, { headers: headers }).then(function (response) {
      if (!response.ok) {
        var message =
          STATUS_MESSAGES[response.status] ||
          "Request failed (status " + response.status + ").";
        var error = new Error(message);
        error.status = response.status;
        throw error;
      }
      return response.json();
    });
  }

  function clearChildren(el) {
    while (el.firstChild) {
      el.removeChild(el.firstChild);
    }
  }

  function renderMessage(el, text) {
    clearChildren(el);
    var p = document.createElement("p");
    p.textContent = text;
    el.appendChild(p);
  }

  function appendCell(row, text, tag) {
    var cell = document.createElement(tag || "td");
    cell.textContent = fmt(text);
    row.appendChild(cell);
  }

  function buildTable(headers, rows) {
    var table = document.createElement("table");
    var thead = document.createElement("thead");
    var headRow = document.createElement("tr");
    headers.forEach(function (h) {
      appendCell(headRow, h, "th");
    });
    thead.appendChild(headRow);
    table.appendChild(thead);
    var tbody = document.createElement("tbody");
    rows.forEach(function (values) {
      var row = document.createElement("tr");
      values.forEach(function (v) {
        appendCell(row, v, "td");
      });
      tbody.appendChild(row);
    });
    table.appendChild(tbody);
    return table;
  }

  function scenarioValue() {
    return qs("scenario-input").value.trim();
  }

  // ---- whoami --------------------------------------------------------

  function refreshWhoAmI() {
    var status = qs("whoami-status");
    if (!getToken()) {
      status.textContent = "Not connected.";
      return;
    }
    apiFetch("/v1/whoami")
      .then(function (me) {
        var scopes =
          me.scopes && me.scopes.length ? me.scopes.join(", ") : "none";
        status.textContent =
          "Connected as " +
          fmt(me.name) +
          " (" +
          fmt(me.actor_kind) +
          ") — scopes: " +
          scopes;
      })
      .catch(function (err) {
        status.textContent = err.message;
      });
  }

  // ---- recommendations -------------------------------------------------

  function refreshRecommendations() {
    var body = qs("recommendations-body");
    if (!getToken()) {
      renderMessage(body, "Connect to load recommendations.");
      return;
    }
    var url = buildUrl("/v1/recommendations", {
      scenario_id: scenarioValue(),
      limit: 200,
    });
    apiFetch(url)
      .then(function (data) {
        renderRecommendations(body, data);
      })
      .catch(function (err) {
        renderMessage(body, err.message);
      });
  }

  function renderRecommendations(body, data) {
    clearChildren(body);
    var recos = data.recommendations || [];
    if (recos.length === 0) {
      renderMessage(body, "No recommendations.");
      return;
    }
    var groups = {};
    recos.forEach(function (r) {
      var level = r.decision_level || "?";
      groups[level] = groups[level] || [];
      groups[level].push(r);
    });
    Object.keys(groups)
      .sort()
      .forEach(function (level) {
        var heading = document.createElement("h3");
        heading.textContent = level + " (" + groups[level].length + ")";
        body.appendChild(heading);
        var rows = groups[level].map(function (r) {
          return [
            r.item_external_id,
            r.action,
            r.status,
            r.shortage_date,
            r.deficit_qty,
            r.recommended_qty,
            r.estimated_cost,
            r.currency,
            r.confidence,
          ];
        });
        body.appendChild(
          buildTable(
            [
              "item",
              "action",
              "status",
              "shortage_date",
              "deficit_qty",
              "recommended_qty",
              "estimated_cost",
              "currency",
              "confidence",
            ],
            rows
          )
        );
      });
    var total = document.createElement("p");
    total.textContent = "Total: " + fmt(data.total);
    body.appendChild(total);
  }

  // ---- outcomes summary (5 proof KPIs) ----------------------------------

  function refreshKpis() {
    var body = qs("kpi-body");
    if (!getToken()) {
      renderMessage(body, "Connect to load KPIs.");
      return;
    }
    var url = buildUrl("/v1/outcomes/summary", {
      scenario_id: scenarioValue(),
    });
    apiFetch(url)
      .then(function (data) {
        renderKpis(body, data);
      })
      .catch(function (err) {
        renderMessage(body, err.message);
      });
  }

  function renderKpis(body, data) {
    clearChildren(body);
    var rows = [
      [
        "pct_shortages_avoided",
        data.pct_shortages_avoided,
        data.avoided_basis_count,
      ],
      ["avoided_severity_usd_total", data.avoided_severity_usd_total, ""],
      ["avg_fva_wape", data.avg_fva_wape, data.fva_basis_count],
      ["reco_approval_rate", data.reco_approval_rate, data.reco_total_count],
      ["cost_of_inaction_usd", data.cost_of_inaction_usd, ""],
    ];
    body.appendChild(buildTable(["KPI", "value", "basis_count"], rows));
    var window_p = document.createElement("p");
    window_p.textContent =
      "Observation window: " + fmt(data.from_date) + " → " + fmt(data.to_date);
    body.appendChild(window_p);
  }

  // ---- scenario compare --------------------------------------------------

  function runCompare() {
    var body = qs("compare-body");
    var ids = qs("compare-ids-input").value.trim();
    if (!getToken()) {
      renderMessage(body, "Connect to compare scenarios.");
      return;
    }
    if (!ids) {
      renderMessage(body, "Enter 2-5 comma-separated scenario ids.");
      return;
    }
    var url = buildUrl("/v1/scenarios/compare", { ids: ids });
    apiFetch(url)
      .then(function (data) {
        renderCompare(body, data);
      })
      .catch(function (err) {
        renderMessage(body, err.message);
      });
  }

  function renderCompare(body, data) {
    clearChildren(body);
    var summary = document.createElement("p");
    summary.textContent =
      "comparable: " +
      fmt(data.comparable) +
      " — reference: " +
      fmt(data.reference_scenario_id) +
      " — cost_precedence: " +
      fmt(data.cost_precedence);
    body.appendChild(summary);
    var rows = (data.entries || []).map(function (e) {
      return [
        e.name,
        e.status,
        e.computable,
        e.stale,
        e.note,
        e.kpis ? e.kpis.shortage_count : null,
        e.kpis ? e.kpis.shortage_severity_usd : null,
        e.kpis ? e.kpis.stock_value_usd : null,
        e.kpis ? e.kpis.fill_rate_est : null,
        e.deltas ? e.deltas.shortage_count_delta : null,
        e.deltas ? e.deltas.severity_usd_delta : null,
      ];
    });
    body.appendChild(
      buildTable(
        [
          "name",
          "status",
          "computable",
          "stale",
          "note",
          "shortages",
          "severity_usd",
          "stock_value_usd",
          "fill_rate_est",
          "Δshortages",
          "Δseverity_usd",
        ],
        rows
      )
    );
  }

  // ---- wiring -------------------------------------------------------------

  function refreshAll() {
    refreshWhoAmI();
    refreshRecommendations();
    refreshKpis();
  }

  function resetPanels() {
    renderMessage(qs("recommendations-body"), "Not loaded.");
    renderMessage(qs("kpi-body"), "Not loaded.");
    renderMessage(qs("compare-body"), "Not loaded.");
  }

  document.addEventListener("DOMContentLoaded", function () {
    qs("connect-button").addEventListener("click", function () {
      var value = qs("token-input").value.trim();
      if (!value) {
        qs("whoami-status").textContent = "Enter a token first.";
        return;
      }
      setToken(value);
      qs("token-input").value = "";
      refreshAll();
    });

    qs("disconnect-button").addEventListener("click", function () {
      clearToken();
      qs("whoami-status").textContent = "Not connected.";
      resetPanels();
    });

    qs("refresh-button").addEventListener("click", refreshAll);
    qs("compare-button").addEventListener("click", runCompare);

    if (getToken()) {
      refreshAll();
    }
  });
})();

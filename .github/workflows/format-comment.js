// OnlyMetrix CI — PR comment formatter.
//
// Pure function of the `omx-ci-output.json` payload produced by
// `omx ci check`. No GitHub API calls here — the calling workflow
// step owns comment create/update/dedup.
//
// No emoji — text indicators only. Some terminals/notifiers strip
// or garble emoji; text is safe everywhere.
//
// Local testing:
//   node .github/workflows/format-comment.js path/to/omx-ci-output.json
//
// Loaded from the workflow via:
//   const { renderComment } = require('./.github/workflows/format-comment.js')

'use strict';

const SEP = '---';
const MARKER = 'OnlyMetrix CI'; // used by the workflow to dedup comments

function renderComment(output) {
  const parts = [];
  const summary = output.summary || {};
  const breaking = output.breaking || [];
  const warnings = output.warnings || [];
  const newModels = output.new_models || [];
  const removedModels = output.removed_models || [];

  const status = output.status || 'clean';
  const header = renderHeader(status);
  parts.push(header);

  if (status === 'clean') {
    parts.push(renderCleanBody(summary));
  } else {
    parts.push(renderSummaryTable(summary));
    for (const b of breaking) {
      parts.push(SEP);
      parts.push(renderChange(b, 'FAIL'));
    }
    for (const w of warnings) {
      parts.push(SEP);
      parts.push(renderChange(w, 'WARN'));
    }
  }

  if (removedModels.length > 0) {
    parts.push(SEP);
    parts.push(renderRemovedModels(removedModels));
  }

  for (const m of newModels) {
    parts.push(SEP);
    parts.push(renderNewModel(m));
  }

  parts.push(SEP);
  parts.push(renderFooter(output));

  return parts.join('\n\n');
}

function renderHeader(status) {
  if (status === 'clean') {
    return `## ${MARKER} [PASS]`;
  }
  if (status === 'warning') {
    return `## ${MARKER} [WARN]\n\nMetric warnings detected. Not blocking — review before merge.`;
  }
  if (status === 'error') {
    // --strict + no baseline
    return `## ${MARKER} [FAIL]\n\nNo baseline available. Run \`omx ci snapshot\` and commit \`.omx/ir.lock.json\`, or configure OMX_API_URL/OMX_API_KEY secrets.`;
  }
  // blocking
  return `## ${MARKER} [FAIL]\n\nThis PR has breaking metric changes. Resolve before merging.`;
}

function renderCleanBody(summary) {
  const checked = summary.models_checked || 0;
  const newN = summary.new || 0;
  const removedN = summary.removed || 0;
  const line1 = `${checked} models checked. No breaking changes.`;
  const extras = [];
  if (newN > 0) extras.push(`${newN} new`);
  if (removedN > 0) extras.push(`${removedN} removed`);
  return extras.length > 0 ? `${line1}\n\n(${extras.join(', ')})` : line1;
}

function renderSummaryTable(summary) {
  const unaffected = Math.max(0, (summary.existing || 0) - (summary.breaking || 0) - (summary.warnings || 0));
  const rows = [
    `Summary`,
    ``,
    `Unaffected: ${unaffected}`,
    `Warnings:   ${summary.warnings || 0}`,
    `Blocking:   ${summary.breaking || 0}`,
  ];
  if ((summary.new || 0) > 0) rows.push(`New models: ${summary.new}`);
  return rows.join('\n');
}

function renderChange(change, severity) {
  const lines = [];
  const metric = change.metric || '(unknown)';
  const tier = change.tier || 'standard';
  lines.push(`[${severity}] ${metric} — ${tier} tier`);
  lines.push('');

  const model = change.model || '';
  if (change.change_type === 'column_possibly_renamed') {
    const conf = change.confidence ? ` (confidence: ${change.confidence})` : '';
    lines.push(`Change: \`${change.old_column}\` possibly renamed to \`${change.new_column}\` in \`${model}\`${conf}`);
  } else if (change.change_type === 'column_dropped') {
    lines.push(`Change: \`${change.old_column}\` column dropped in \`${model}\``);
  } else {
    lines.push(`Change: ${change.change_type || 'unknown'} in \`${model}\``);
  }

  lines.push('');
  lines.push('Impact:');
  const impact = change.impact || {};
  const canvasN = impact.canvas_dashboards || 0;
  const canvasTitles = impact.canvas_titles || [];
  if (canvasN > 0) {
    lines.push(`  Dashboards affected: ${canvasN}`);
    for (const t of canvasTitles) lines.push(`    ${t}`);
  } else {
    lines.push(`  Dashboards affected: 0`);
  }

  // Decisions line rules (see workflow YAML comment).
  // canvas_dashboards > 0 + cloud + decisions available -> "Decisions at risk: N" + list
  // canvas_dashboards > 0 + not cloud                   -> "Decisions at risk: decision tracking requires OnlyMetrix cloud"
  // canvas_dashboards == 0                              -> OMIT the line entirely
  if (canvasN > 0) {
    const decisionsN = impact.recent_decisions || 0;
    const decisionsList = impact.decisions || [];
    const note = impact.decisions_note || '';
    if (decisionsN > 0) {
      lines.push(`  Decisions at risk: ${decisionsN}`);
      for (const d of decisionsList.slice(0, 3)) {
        const text = d.text || d.summary || '(decision)';
        lines.push(`    ${text}`);
      }
      if (decisionsList.length > 3) {
        lines.push(`    ...and ${decisionsList.length - 3} more`);
      }
    } else if (note.toLowerCase().includes('cloud')) {
      // Intentional conversion touchpoint. See workflow YAML docstring.
      lines.push(`  Decisions at risk: decision tracking requires OnlyMetrix cloud`);
    }
  }

  if (change.fix) {
    lines.push('');
    lines.push(`Fix: \`${change.fix}\``);
  }
  return lines.join('\n');
}

function renderRemovedModels(removed) {
  const lines = ['[FAIL] Models removed from manifest', ''];
  for (const r of removed) {
    lines.push(`  ${r.model || '(unknown)'}`);
  }
  lines.push('');
  lines.push('Removed models still have metrics referencing them. Run `omx metrics deprecate <name>` for each affected metric.');
  return lines.join('\n');
}

function renderNewModel(model) {
  const modelName = model.model || '(unknown)';
  const proposals = model.proposed_metrics || [];
  const lines = [`[INFO] New model: ${modelName}`, ''];
  if (proposals.length === 0) {
    lines.push(`No metric proposals generated. Run \`omx discover --table ${modelName}\` to explore live data.`);
    return lines.join('\n');
  }
  lines.push(`${proposals.length} ${proposals.length === 1 ? 'metric proposed' : 'metrics proposed'}`);
  lines.push('');
  lines.push('```');
  lines.push(renderProposalTable(proposals));
  lines.push('```');
  const hasCore = proposals.some(p => (p.tier || '').toLowerCase() === 'core' || (p.tier || '').toLowerCase() === 'critical');
  if (hasCore) {
    lines.push('');
    lines.push('Note: core tier proposals require review before use.');
  }
  const hasCurrencyNote = proposals.some(p => (p.notes || []).length > 0);
  if (hasCurrencyNote) {
    lines.push('');
    lines.push('Notes:');
    for (const p of proposals) {
      for (const n of (p.notes || [])) {
        lines.push(`  ${p.name}: ${n}`);
      }
    }
  }
  lines.push('');
  lines.push(`Accept: \`omx discover --table ${modelName}\``);
  return lines.join('\n');
}

function renderProposalTable(proposals) {
  const headers = ['Name', 'Type', 'Tier', 'Confidence'];
  const rows = proposals.map(p => [
    p.name || '',
    p.type || '',
    p.tier || '',
    p.confidence || '',
  ]);
  const widths = headers.map((h, i) =>
    Math.max(h.length, ...rows.map(r => String(r[i]).length))
  );
  const fmtRow = (r) => r.map((c, i) => String(c).padEnd(widths[i])).join('  ');
  return [fmtRow(headers), ...rows.map(fmtRow)].join('\n');
}

function renderFooter(output) {
  const src = output.baseline_source || 'none';
  const note = src === 'api'
    ? 'Baseline: fetched from API'
    : src === 'lockfile'
      ? 'Baseline: .omx/ir.lock.json'
      : 'Baseline: none (configure OMX_API_URL/OMX_API_KEY or commit .omx/ir.lock.json)';
  return `_${note}_`;
}

module.exports = { renderComment, MARKER };

// CLI entrypoint: `node format-comment.js path/to/omx-ci-output.json`
if (require.main === module) {
  const fs = require('fs');
  const file = process.argv[2];
  if (!file) {
    console.error('Usage: node format-comment.js <omx-ci-output.json>');
    process.exit(2);
  }
  const output = JSON.parse(fs.readFileSync(file, 'utf8'));
  process.stdout.write(renderComment(output) + '\n');
}

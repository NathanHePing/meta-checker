// src/lib/output-gating.js

function validateOutputs(shape, selections) {
  const errors = [];
  const { exists, firstColUrlShare, inferredRoles } = shape || {};
  const hasInput = !!exists;
  const urlishEnough = (firstColUrlShare || 0) >= 0.6;

  const wants = new Set(selections || []);
  // Always allowed: urls, site_catalog, internal_links, tree
  // Gated: existence_csv requires input + URL-ish first column
  if (wants.has('existence_csv')) {
    if (!hasInput) errors.push("existence_csv requires an input file.");
    if (!urlishEnough) errors.push("existence_csv requires first column to be URL-ish in ≥60% rows.");
  }

  // Gated: comparison_csv requires input + has (title and/or description)
  if (wants.has('comparison_csv')) {
    if (!hasInput) errors.push("comparison_csv requires an input file.");
    const roles = (inferredRoles && inferredRoles[0]) || [];
    const hasTitleOrDesc = roles.includes("title") || roles.includes("description");
    if (!hasTitleOrDesc) {
      errors.push("comparison_csv requires title and/or description columns (inferred from 2–3 column inputs).");
    }
  }

  // If no input: forbid both gated
  if (!hasInput) {
    if (wants.has('existence_csv') || wants.has('comparison_csv')) {
      // already pushed errors above
    }
  }

  return {
    ok: errors.length === 0,
    errors
  };
}

module.exports = { validateOutputs };

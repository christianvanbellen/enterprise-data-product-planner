# Conformed Schema Artifact Spec

**Artifact name:** `conformed_schema.json` (also shipped as `output_schema_conformed_data_only_-From_Robbie-06Mar2025.json`)
**Current version confirmed against:** JSON Schema draft-04
**Current producer:** the warehouse modelling architect (authored manually)

---

## What this artifact is

The conformed schema is a **JSON Schema draft-04 document used as a vocabulary carrier**,
not as a validation target. The file declares the canonical business concepts recognised
in the warehouse — grouped as `coverage`, `policy`, `rate_monitoring`, and their `_totals`
currency-sharded variants — along with the canonical name and JSON Schema type of every
field within each group.

The pipeline consumes only the **structural shape** of this document (the nested `properties`
keys) to build a registry of business terms. It does not validate input data against the
schema. The field-level `"type"` annotations are treated as hints — stored on each term's
`attributes.data_type` — but are not used by any downstream phase for type coercion.

This file is the most architecturally significant input to the pipeline. It defines which
business concepts the analytics layer can formally recognise and bind assets to; it is the
source of every `BusinessEntityNode` in the semantic graph. See `ARCHITECTURE.md` — The
two-model architecture.

---

## Root structure

```jsonc
{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "type": "array",
  "items": {
    "type": "object",
    "properties": {
      "conformed_data": {
        "type": "object",
        "properties": {
          "<entity_group>": { ... },   // one entry per registered business concept group
          ...
        }
      }
    }
  }
}
```

The adapter navigates `root.items.properties.conformed_data.properties` to reach the dict
of registered groups. Nothing above that level is read — `$schema` and the outer `type: array`
envelope are informational only. See `ingestion/adapters/conformed_schema.py` — `_extract_entity_groups()`.

---

## Entity group — two shapes

Every entry under `conformed_data.properties` is either an **array group** or an
**object-of-objects group**. The adapter dispatches on the `"type"` field of each group.

### Shape 1 — array group

Used when one conceptual group has a flat list of canonical fields. All current
non-totals groups (`coverage`, `policy`, `profitability_measures`, `rate_monitoring`)
follow this shape.

```jsonc
"coverage": {
  "type": "array",
  "items": {
    "type": "object",
    "properties": {
      "filter":            { "type": "boolean" },
      "quote_name":        { "type": "string" },
      "section":           { "type": "string" },
      "coverage":          { "type": "string" },
      "inception_date":    { "type": "string" },
      "expiry_date":       { "type": "string" },
      "policy_currency":   { "type": "string" },
      "exposure":          { "type": "integer" },
      "limit_100":         { "type": "integer" },
      "excess":            { "type": "number" },
      "deductible_value":  { "type": "integer" },
      "tech_elc":          { "type": "null" },
      ...
    }
  }
}
```

Fields are read from `items.properties`. Every key becomes one child `CanonicalBusinessTerm`
parented to a single group-level term named after the top-level key.

### Shape 2 — object-of-objects group

Used when one conceptual group is sharded into sub-groups by currency or share basis.
All current `_totals` groups (`policy_totals`, `profitability_measures_totals`,
`rate_monitoring_totals`) follow this shape.

```jsonc
"policy_totals": {
  "type": "object",
  "properties": {
    "100_percent_original_ccy": {
      "type": "object",
      "properties": {
        "commission":    { "type": "number" },
        "tech_gnwp":     { "type": "number" },
        "sold_gnwp":     { "type": "number" },
        ...
      }
    },
    "lsm_share_original_ccy":  { "type": "object", "properties": {...} },
    "100_percent_usd":         { "type": "object", "properties": {...} },
    "lsm_share_usd":           { "type": "object", "properties": {...} }
  }
}
```

The adapter detects this shape when every value under `properties` is itself an
`{"type": "object", "properties": {...}}` block. It emits:
- One top-level group term (e.g. `policy_totals`)
- One sub-group term per nested object (e.g. `100_percent_original_ccy`), parented to the top-level group
- One field term per leaf field, parented to its sub-group

The field-level terms therefore live three levels deep in the term hierarchy. See
`_emit_group_and_fields()` for the exact walk.

---

## Field object — what is actually present

Every leaf field — regardless of which shape it lives under — is a single object with a
`"type"` key and, in practice, nothing else:

```jsonc
{
  "type": "number"
}
```

The seven JSON Schema types observed in the current file:

| type       | meaning in this artifact                                          |
|------------|-------------------------------------------------------------------|
| `string`   | Categorical or textual field                                      |
| `integer`  | Discrete numeric field (no decimals expected)                     |
| `number`   | Continuous numeric field — the dominant measure type              |
| `boolean`  | True/false flag (the `filter` field on every group)               |
| `null`     | Placeholder: field is named but its type is not yet declared      |
| `object`   | Nesting marker (shape-2 sub-groups only)                          |
| `array`    | Shape-1 group marker (only appears at the group level)            |

**`"type": "null"` is the most important variant to understand.** It does not mean the
field is nullable — it means the architect has reserved the name as a canonical term
without committing to a type. In the current file, around 20 fields across `coverage`,
`policy`, and `profitability_measures` are declared with `"type": "null"`. They are
still registered as `CanonicalBusinessTerm` objects and still participate in
`ConformedFieldBinder` overlap scoring — the adapter does not filter them out.

**No other JSON Schema keywords are present.** `required`, `enum`, `minLength`, `description`,
`$ref`, `allOf`, `oneOf`, etc. are all absent. The file is flat metadata, not a
validation contract.

---

## How the adapter consumes the file

`ConformedSchemaAdapter` produces zero assets, zero columns, and zero lineage edges. Its
entire output is a list of `CanonicalBusinessTerm` objects — the business vocabulary registry.

For each entity group in the current file:

| Group | Shape | Terms produced |
|-------|-------|----------------|
| `coverage` | array | 1 group + 21 fields = 22 terms |
| `policy` | array | 1 group + 29 fields = 30 terms |
| `policy_totals` | object-of-objects | 1 group + 4 sub-groups + 4 × 27 fields = 113 terms |
| `profitability_measures` | array | 1 group + 26 fields = 27 terms |
| `profitability_measures_totals` | object-of-objects | 1 group + 4 sub-groups + 4 × 21 fields = 89 terms |
| `rate_monitoring` | array | 1 group + 29 fields = 30 terms |
| `rate_monitoring_totals` | object-of-objects | 1 group + 4 sub-groups + 4 × 19 fields = 81 terms |
| **Total** | | **~392 terms** (varies with field counts) |

**Per-term fields on `CanonicalBusinessTerm`:**

| field | value on group terms | value on field terms |
|-------|---------------------|----------------------|
| `internal_id` | `term_<stable_hash>` derived from path tokens | same, but with field name appended |
| `term_type` | `"conformed_concept"` | `"conformed_concept"` |
| `name` | group key as-is | field key as-is |
| `normalized_name` | snake-case lowercased | snake-case lowercased |
| `parent_term_id` | `None` for top-level; parent group's ID for sub-groups | parent group's ID |
| `attributes.schema_type` | `"array"` / `"object"` / `"object_of_objects"` | not set |
| `attributes.field_count` | integer count of direct child fields | not set |
| `attributes.data_type` | not set | the `"type"` value: string / number / integer / boolean / null / unknown |
| `version_hash` | SHA-256 of the group sub-tree | SHA-256 of the field definition dict |
| `provenance` | `source_system="conformed_schema"`, `source_type="ConformedSchemaAdapter"` | same |

The hierarchy produced for a shape-2 group looks like:

```
policy_totals                              (parent_term_id=None)
├── 100_percent_original_ccy               (parent_term_id=policy_totals)
│   ├── commission                         (parent_term_id=100_percent_original_ccy)
│   ├── tech_gnwp                          (parent_term_id=100_percent_original_ccy)
│   └── ...
├── lsm_share_original_ccy                 (parent_term_id=policy_totals)
│   ├── commission
│   └── ...
├── 100_percent_usd                        (parent_term_id=policy_totals)
└── lsm_share_usd                          (parent_term_id=policy_totals)
```

---

## How these terms flow into Phase 3 binding

`ConformedFieldBinder` in Phase 3 reads two things from the bundle:

1. **Group names** — it looks up every name in `ENTITY_GROUPS` (from `ontology/entity_groups.yaml`)
   against `CanonicalBusinessTerm.name` for top-level terms (where `parent_term_id is None`).
2. **Group field sets** — for each matched group, it collects every child term's `name` as
   the canonical field set for overlap scoring.

An asset binds to a group when the intersection of its column normalised names with the
group's canonical field names meets `OVERLAP_THRESHOLD` (default 0.5). Bound assets get
a `REPRESENTS` edge to the corresponding `BusinessEntityNode` in the semantic graph.

**Only the top-level group names in `ENTITY_GROUPS` are consulted.** Sub-group field sets
(e.g. the 27 fields under `policy_totals.100_percent_original_ccy`) are registered as
terms in the bundle but are not scanned by the binder unless their parent group is in
`ENTITY_GROUPS`. This is the reason `policy_totals` is currently registered but
`100_percent_original_ccy` is not — the binder only matches at the top level.

---

## What the adapter does NOT handle

- **JSON Schema validation keywords**: `required`, `enum`, `minLength`, `pattern`,
  `$ref`, `allOf`, `oneOf`, `anyOf`, `not`, `additionalProperties`, `description`,
  `title`, `format`, `default` are all silently ignored. Only `type` and nested
  `properties` / `items` are read.
- **Cross-file schema references (`$ref`)**: not resolved. If a field's value is a
  `$ref` rather than a type dict, it will be passed through to `attributes` as-is
  and its `data_type` will be `"unknown"`.
- **Groups deeper than two levels**: the adapter walks `conformed_data.properties →
  array items OR object-of-objects`. A three-level nesting (object containing objects
  containing more objects) would have its innermost layer treated as leaf field
  definitions, which may produce surprising term structures.
- **Multiple top-level `conformed_data` entries**: the root is technically an array
  (`"type": "array"`), but the adapter only reads the first entry's schema via
  `items.properties`. If `items` were an array of schemas (tuple validation),
  only the first would be read.
- **Field-level documentation**: no `description` or `title` is captured. Business
  glossary descriptions for canonical terms live elsewhere and are not in this file.

---

## The schema gap that matters most

The single highest-leverage edit to this file is **adding a `pricing_component` group**.
The `pricing_decomposition` capability primitive requires five columns — `commission`,
`modtech_gnwp`, `sold_gnwp`, `tech_elc`, `tech_gnwp` — that currently live as child fields
under `policy` and `rate_monitoring`. Because they are parented to those groups rather
than to a dedicated `pricing_component` group, `CapabilityPrimitiveExtractor` finds no
supporting assets for `pricing_decomposition` (it requires the `pricing_component` entity).
The primitive appears as **inferred** (amber hexagon) rather than **confirmed**.

To resolve:
1. Add a new top-level group under `conformed_data.properties`:
   ```jsonc
   "pricing_component": {
     "type": "array",
     "items": {
       "type": "object",
       "properties": {
         "commission":   { "type": "number" },
         "tech_gnwp":    { "type": "number" },
         "modtech_gnwp": { "type": "number" },
         "sold_gnwp":    { "type": "number" },
         "tech_elc":     { "type": "number" }
       }
     }
   }
   ```
2. Add `"pricing_component"` to `ontology/entity_groups.yaml`.
3. Re-run Phase 1 → 5.

Seven initiatives move from amber to green as a result. See `ARCHITECTURE.md` —
The pricing_decomposition gap, and `docs/backlog.md` for the full remediation plan.

---

## Sample minimal valid file

The following is the minimum structure the adapter recognises and parses without warnings:

```json
{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "type": "array",
  "items": {
    "type": "object",
    "properties": {
      "conformed_data": {
        "type": "object",
        "properties": {
          "coverage": {
            "type": "array",
            "items": {
              "type": "object",
              "properties": {
                "quote_id":       { "type": "string" },
                "coverage":       { "type": "string" },
                "exposure":       { "type": "number" },
                "limit_100":      { "type": "number" },
                "inception_date": { "type": "string" }
              }
            }
          },
          "policy_totals": {
            "type": "object",
            "properties": {
              "100_percent_usd": {
                "type": "object",
                "properties": {
                  "commission":  { "type": "number" },
                  "tech_gnwp":   { "type": "number" },
                  "sold_gnwp":   { "type": "number" }
                }
              }
            }
          }
        }
      }
    }
  }
}
```

This file produces 1 + 5 + 1 + 1 + 3 = **11 `CanonicalBusinessTerm` objects**: one for the
`coverage` group, five for its child fields, one for the `policy_totals` group, one for the
`100_percent_usd` sub-group, and three for that sub-group's leaf fields.

---

## Current file summary (Liberty warehouse, March 2025)

- 7 top-level groups
- 12 sub-groups (across 3 `_totals` groups, each with 4 currency/share shards)
- ~380 leaf fields
- 392 total `CanonicalBusinessTerm` objects produced
- 0 `description` or other metadata fields — the file is pure structure + type hints

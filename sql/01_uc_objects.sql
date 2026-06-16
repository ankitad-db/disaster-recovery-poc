-- UC structure for the DR POC. Run in BOTH metastores (west + east) so the
-- target namespace exists before any import. Names mirror config/dr_config.yaml.

CREATE CATALOG IF NOT EXISTS dr_poc
  COMMENT 'Disaster Recovery POC catalog (models module)';

CREATE SCHEMA IF NOT EXISTS dr_poc.ml
  COMMENT 'Replicated models, experiments and runs';

CREATE SCHEMA IF NOT EXISTS dr_poc.dr_control
  COMMENT 'DR control plane: audit table and watermarks';

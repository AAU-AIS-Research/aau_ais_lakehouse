-- already present (drops the tables first, removing the dependency on each sequence)
DROP TABLE IF EXISTS ais.dim.gap_type_dim;
DROP TABLE IF EXISTS ais.dim.gap_imp_mv_dim;
DROP TABLE IF EXISTS lakehouse.fact.ais_gap_fact;

-- now safe to drop the sequences
DROP SEQUENCE IF EXISTS ais.dim.gap_type_dim_seq;
DROP SEQUENCE IF EXISTS ais.dim.gap_explanation_dim_seq;
DROP SEQUENCE IF EXISTS ais.dim.gap_imp_geom_dim_seq;
DROP SEQUENCE IF EXISTS ais.dim.gap_imp_method_dim_seq;
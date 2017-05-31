-- Table: hgd."GeneAliases"

-- DROP TABLE hgd."GeneAliases";

CREATE TABLE hgd."GeneAliases"
(
  "GeneSymbol" character varying(50),
  "Alias" character varying(50)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE hgd."GeneAliases"
  OWNER TO postgres;

-- Table: hgd."Genes"

-- DROP TABLE hgd."Genes";

CREATE TABLE hgd."Genes"
(
  "Symbol" character varying(50),
  "Name" character varying(250)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE hgd."Genes"
  OWNER TO postgres;

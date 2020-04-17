DROP TABLE IF EXISTS transcript_mv;
CREATE TABLE transcript_mv (
    gene INT NOT NULL
  , tissue INT NOT NULL
  , count_avg    DECIMAL(10,2) NOT NULL
  , UNIQUE INDEX product (gene, tissue)
);
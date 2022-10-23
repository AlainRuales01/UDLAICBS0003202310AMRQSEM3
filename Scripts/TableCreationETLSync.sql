USE amrqdbstg;
CREATE TABLE ETLSync
    (
     ID INTEGER  NOT NULL auto_increment primary key, 
     SYNCHRONIZATION_DATE DateTime
    )
;
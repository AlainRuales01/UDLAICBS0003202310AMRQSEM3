USE amrqdbsor;
CREATE TABLE CHANNELS
    (
     ID INTEGER NOT NULL auto_increment primary key, 
     CHANNEL_ID INTEGER  NOT NULL , 
     CHANNEL_DESC VARCHAR (20)  NOT NULL , 
     CHANNEL_CLASS VARCHAR (20)  NOT NULL , 
     CHANNEL_CLASS_ID INTEGER NOT NULL 
    )
;
CREATE TABLE COUNTRIES 
    ( 
     ID INTEGER NOT NULL auto_increment primary key, 
     COUNTRY_ID INTEGER  NOT NULL , 
     COUNTRY_NAME VARCHAR (40)  NOT NULL , 
     COUNTRY_REGION VARCHAR (20)  NOT NULL , 
     COUNTRY_REGION_ID INTEGER  NOT NULL 
    ) 
;



CREATE TABLE CUSTOMERS 
    ( 
	 ID INTEGER NOT NULL auto_increment primary key, 
     CUST_ID INTEGER  NOT NULL , 
     CUST_FIRST_NAME VARCHAR (20)  NOT NULL , 
     CUST_LAST_NAME VARCHAR (40)  NOT NULL , 
     CUST_GENDER CHAR (1)  NOT NULL , 
     CUST_YEAR_OF_BIRTH INTEGER (4)  NOT NULL , 
     CUST_MARITAL_STATUS VARCHAR (20) , 
     CUST_STREET_ADDRESS VARCHAR (40)  NOT NULL , 
     CUST_POSTAL_CODE VARCHAR (10)  NOT NULL , 
     CUST_CITY VARCHAR (30)  NOT NULL , 
     CUST_STATE_PROVINCE VARCHAR (40)  NOT NULL , 
     COUNTRY_ID INTEGER NOT NULL , 
     CUST_MAIN_PHONE_INTEGER VARCHAR (25)  NOT NULL , 
     CUST_INCOME_LEVEL VARCHAR (30) , 
     CUST_CREDIT_LIMIT INTEGER , 
     CUST_EMAIL VARCHAR (30)
    )
;


CREATE TABLE PRODUCTS 
    ( 
     ID INTEGER NOT NULL auto_increment primary key, 
     PROD_ID INTEGER   NOT NULL , 
     PROD_NAME VARCHAR (50)  NOT NULL , 
     PROD_DESC VARCHAR (4000)  NOT NULL , 
     PROD_CATEGORY VARCHAR (50)  NOT NULL , 
     PROD_CATEGORY_ID INTEGER  NOT NULL , 
     PROD_CATEGORY_DESC VARCHAR (2000)  NOT NULL , 
     PROD_WEIGHT_CLASS INTEGER (3)  NOT NULL , 
     SUPPLIER_ID INTEGER (6)  NOT NULL , 
     PROD_STATUS VARCHAR (20)  NOT NULL , 
     PROD_LIST_PRICE DECIMAL (8,2)  NOT NULL , 
     PROD_MIN_PRICE DECIMAL (8,2)  NOT NULL 
    )
;

CREATE TABLE PROMOTIONS 
    ( 
     ID INTEGER NOT NULL auto_increment primary key, 
     PROMO_ID INTEGER NOT NULL , 
     PROMO_NAME VARCHAR (30)  NOT NULL , 
     PROMO_COST DECIMAL (10,2)  NOT NULL , 
     PROMO_BEGIN_DATE DATE  NOT NULL , 
     PROMO_END_DATE DATE  NOT NULL 
    )
;
CREATE TABLE SALES 
    ( 
     ID INTEGER NOT NULL auto_increment primary key, 
     PROD_ID INTEGER  NOT NULL , 
     CUST_ID INTEGER  NOT NULL , 
     TIME_ID INTEGER  NOT NULL , 
     CHANNEL_ID INTEGER  NOT NULL , 
     PROMO_ID INTEGER NOT NULL , 
     QUANTITY_SOLD DECIMAL (10,2)  NOT NULL , 
     AMOUNT_SOLD DECIMAL (10,2)  NOT NULL 
    ) 
;

CREATE TABLE TIMES 
    ( 
     ID INTEGER NOT NULL auto_increment primary key, 
     TIME_ID DATE  NOT NULL , 
     DAY_NAME VARCHAR (9)  NOT NULL , 
     DAY_INTEGER_IN_WEEK INTEGER (1)  NOT NULL , 
     DAY_INTEGER_IN_MONTH INTEGER (2)  NOT NULL , 
     CALENDAR_WEEK_INTEGER INTEGER (2)  NOT NULL , 
     CALENDAR_MONTH_INTEGER INTEGER (2)  NOT NULL , 
     CALENDAR_MONTH_DESC VARCHAR (8)  NOT NULL , 
     END_OF_CAL_MONTH DATE  NOT NULL , 
     CALENDAR_MONTH_NAME VARCHAR (9)  NOT NULL , 
     CALENDAR_QUARTER_DESC CHAR (7)  NOT NULL , 
     CALENDAR_YEAR INTEGER (4)  NOT NULL 
    ) 
;
ALTER TABLE CUSTOMERS 
    ADD CONSTRAINT CUSTOMERS_COUNTRY_FK FOREIGN KEY 
    ( 
     COUNTRY_ID
    ) 
    REFERENCES COUNTRIES 
    ( 
     ID
    )
;


ALTER TABLE SALES 
    ADD CONSTRAINT SALES_CHANNEL_FK FOREIGN KEY 
    ( 
     CHANNEL_ID
    ) 
    REFERENCES CHANNELS 
    ( 
     ID
    ) 
     
;


ALTER TABLE SALES 
    ADD CONSTRAINT SALES_CUSTOMER_FK FOREIGN KEY 
    ( 
     CUST_ID
    ) 
    REFERENCES CUSTOMERS 
    ( 
     ID
    ) 
     
;


ALTER TABLE SALES 
    ADD CONSTRAINT SALES_PRODUCT_FK FOREIGN KEY 
    ( 
     PROD_ID
    ) 
    REFERENCES PRODUCTS 
    ( 
     ID
    ) 
     
;


ALTER TABLE SALES 
    ADD CONSTRAINT SALES_PROMO_FK FOREIGN KEY 
    ( 
     PROMO_ID
    ) 
    REFERENCES PROMOTIONS 
    ( 
     ID
    ) 
     
;


ALTER TABLE SALES 
    ADD CONSTRAINT SALES_TIME_FK FOREIGN KEY 
    ( 
     TIME_ID
    ) 
    REFERENCES TIMES 
    ( 
     ID
    ) 
     
;

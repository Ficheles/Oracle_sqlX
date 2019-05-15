CREATE OR REPLACE PROCEDURE CREATE_FILE_CSV(P_PATH_DIR IN VARCHAR2, P_FILE_NAME IN VARCHAR, SQL_CUR IN OUT SYS_REFCURSOR )  IS
  W_FILE          UTL_FILE.FILE_TYPE;
  SRC_CUR         SYS_REFCURSOR;
  CUR_ID          INTEGER;
  DESCTAB         DBMS_SQL.DESC_TAB;
  COLCNT          NUMBER;

  CRLF            CHAR(2) := CHR(13)||CHR(10);
  -- Referencia para col_type de DBMS_SQL  https://docs.oracle.com/database/121/ARPLS/d_sql.htm#ARPLS68236
  -- https://stackoverflow.com/questions/36117633/oracle-return-dynamic-result-set-from-sys-refcursor
  -- http://www.java2s.com/Code/Oracle/System-Packages/Passaquerystatementtoastoredprocedure.htm
  -- http://www.java2s.com/Code/Oracle/System-Packages/Calldbmssqldescribecolumns2togetinfoforacolumn.htm

  V_VCH_COL       VARCHAR2(4000);                 -- dbms_types.typecode_varchar2      -- (1,96,11,208) -- v_vc_col;
  V_NUM_COL       NUMBER;                         -- dbms_types.typecode_number        -- 2   -- v_num_col;
  V_DATE_COL      DATE;                           -- dbms_types.typecode_date          -- 12  -- v_date_col;
  V_RAW_COL       RAW(32767);                     -- dbms_types.typecode_raw           -- 23  -- v_raw_col;
  V_INT_DS_COL    INTERVAL DAY TO SECOND;         -- dbms_types.typecode_interval_ds   -- 183 -- v_int_ds_col;
  V_INT_YM_COL    INTERVAL YEAR TO MONTH;         -- dbms_types.typecode_interval_ym   -- 182 -- v_int_ym_col;
  V_TS_COL        TIMESTAMP;                      -- dbms_types.typecode_timestamp     -- 180 -- v_ts_col;
  V_TSTZ_COL      TIMESTAMP WITH TIME ZONE;       -- dbms_types.typecode_timestamp_tz  -- 181 -- v_tstz_col);
  V_TSLTZ_COL     TIMESTAMP WITH LOCAL TIME ZONE; -- dbms_types.typecode_timestamp_ltz -- 231 -- v_tsltz_col);
  V_CLOB_COL      CLOB;                           -- dbms_types.typecode_clob          -- 112 -- v_clob_col
  -- dbms_types.typecode_clob          -- 8   -- dbms_sql.column_value_long(cursorId, i, 32767, v_clob_offset, v_vc_col, v_clob_len);
  V_CLOB_LEN      INTEGER;
  V_CLOB_OFFSET   INTEGER := 0;
  V_RAW_ERROR     NUMBER;
  V_RAW_LEN       INTEGER;


BEGIN

    -- Abertura do arquivo
    BEGIN
        -- Handle of FILE
        W_FILE := UTL_FILE.FOPEN(P_PATH_DIR, P_FILE_NAME, 'W');

    EXCEPTION
        WHEN UTL_FILE.INVALID_OPERATION THEN
           DBMS_OUTPUT.PUT_LINE('Operação inválida no arquivo. ('|| P_FILE_NAME ||').' || CRLF ||SQLERRM);
           UTL_FILE.FCLOSE(W_FILE);
        WHEN UTL_FILE.WRITE_ERROR THEN
           DBMS_OUTPUT.PUT_LINE('Erro de gravação no arquivo ('|| P_FILE_NAME ||').' || CRLF || SQLERRM);
           UTL_FILE.FCLOSE(W_FILE);
        WHEN UTL_FILE.INVALID_PATH THEN
           DBMS_OUTPUT.PUT_LINE('Diretório inválido('|| P_PATH_DIR ||').' || CRLF || SQLERRM);
           UTL_FILE.FCLOSE(W_FILE);
        WHEN UTL_FILE.INVALID_MODE THEN
           DBMS_OUTPUT.PUT_LINE('Modo de acesso inválido. ("W")');
           UTL_FILE.FCLOSE(W_FILE);
        WHEN OTHERS THEN
           DBMS_OUTPUT.PUT_LINE('Problemas na geração do arquivo ('|| P_PATH_DIR || P_FILE_NAME ||').' || CRLF ||SQLERRM);
           UTL_FILE.FCLOSE(W_FILE);
    END;

    -- Inclui os registro retornado no arquivo
    BEGIN
        -- Abre o curso para utilização

        CUR_ID := DBMS_SQL.TO_CURSOR_NUMBER(SQL_CUR);

        --DBMS_SQL.PARSE( CUR_ID,  p_query, dbms_sql.native );
        DBMS_SQL.DESCRIBE_COLUMNS(CUR_ID, COLCNT, DESCTAB);

        -- Define columns
        FOR i IN 1 .. colcnt LOOP
            CASE
                WHEN  DESCTAB(I).COL_TYPE = 2 THEN
                    DBMS_SQL.DEFINE_COLUMN(CUR_ID, I, V_NUM_COL);
                WHEN DESCTAB(I).COL_TYPE = 12 THEN
                   DBMS_SQL.DEFINE_COLUMN(CUR_ID, I, V_DATE_COL);
                WHEN DESCTAB(I).COL_TYPE = 23 THEN
                   DBMS_SQL.DEFINE_COLUMN(CUR_ID, I, V_RAW_COL);
                WHEN DESCTAB(I).COL_TYPE = 112 THEN
                   DBMS_SQL.DEFINE_COLUMN(CUR_ID, I, V_CLOB_COL);
                WHEN DESCTAB(I).COL_TYPE = 183 THEN
                   DBMS_SQL.DEFINE_COLUMN(CUR_ID, I, V_INT_DS_COL);
                WHEN DESCTAB(I).COL_TYPE = 182 THEN
                   DBMS_SQL.DEFINE_COLUMN(CUR_ID, I, V_INT_YM_COL);
                WHEN DESCTAB(I).COL_TYPE = 180 THEN
                   DBMS_SQL.DEFINE_COLUMN(CUR_ID, I, V_TS_COL);
                WHEN DESCTAB(I).COL_TYPE = 181 THEN
                   DBMS_SQL.DEFINE_COLUMN(CUR_ID, I, V_TSTZ_COL);
                WHEN DESCTAB(I).COL_TYPE = 231 THEN
                   DBMS_SQL.DEFINE_COLUMN(CUR_ID, I, V_TSLTZ_COL);
                WHEN DESCTAB(I).COL_TYPE IN ( 1, 96, 11, 208 ) THEN
                   DBMS_SQL.DEFINE_COLUMN(CUR_ID, I, V_VCH_COL, 4000);
                ELSE
                   DBMS_OUTPUT.PUT_LINE(DESCTAB(I).COL_NAME || ': '|| DESCTAB(I).COL_TYPE ||' -> NÃ£o identificado') ;
            END CASE;
        END LOOP;


        -- Inclui o cabeçalho do Arquivo
        FOR i IN 1 .. COLCNT LOOP
            UTL_FILE.PUT(W_FILE, DESCTAB(I).COL_NAME || ';');
           -- DBMS_OUTPUT.PUT(DESCTAB(I).COL_NAME || ';');
        END LOOP;

        -- Insere linha dos Cabeçalhos
        UTL_FILE.NEW_LINE(W_FILE);
        --DBMS_OUTPUT.NEW_LINE();


        --- Percorre os registros
        WHILE DBMS_SQL.FETCH_ROWS(CUR_ID) > 0 LOOP

            -- Inicia linha de dados
            FOR i IN 1 .. COLCNT LOOP

                CASE
                    WHEN  DESCTAB(I).COL_TYPE = 2 THEN
                        DBMS_SQL.COLUMN_VALUE(CUR_ID, I, V_NUM_COL);     -- Atribui valor a variavel
                        UTL_FILE.PUT(W_FILE, V_NUM_COL || ';');
                       -- DBMS_OUTPUT.PUT(V_NUM_COL || ';');
                    WHEN DESCTAB(I).COL_TYPE = 12 THEN
                        DBMS_SQL.COLUMN_VALUE(CUR_ID, I, V_DATE_COL);
                        UTL_FILE.PUT(W_FILE, V_DATE_COL || ';');
                       -- DBMS_OUTPUT.PUT(V_DATE_COL || ';');
                    WHEN DESCTAB(I).COL_TYPE = 23 THEN
                        DBMS_SQL.COLUMN_VALUE(CUR_ID, I, V_RAW_COL);     -- Atribui valor a variavel
                        UTL_FILE.PUT(W_FILE, V_RAW_COL || ';');
                       -- DBMS_OUTPUT.PUT(V_RAW_COL || ';');
                    WHEN DESCTAB(I).COL_TYPE = 112 THEN
                        DBMS_SQL.COLUMN_VALUE(CUR_ID, I, V_CLOB_COL);    -- Atribui valor a variavel
                        UTL_FILE.PUT(W_FILE, V_CLOB_COL || ';');
                       -- DBMS_OUTPUT.PUT(V_CLOB_COL || ';');
                    WHEN DESCTAB(I).COL_TYPE = 183 THEN
                        DBMS_SQL.COLUMN_VALUE(CUR_ID, I, V_INT_DS_COL);  -- Atribui valor a variavel
                        UTL_FILE.PUT(W_FILE, V_INT_DS_COL || ';');
                       -- DBMS_OUTPUT.PUT(V_INT_DS_COL || ';');
                    WHEN DESCTAB(I).COL_TYPE = 182 THEN
                        DBMS_SQL.COLUMN_VALUE(CUR_ID, I, V_INT_YM_COL);  -- Atribui valor a variavel
                        UTL_FILE.PUT(W_FILE, V_INT_YM_COL || ';');
                       -- DBMS_OUTPUT.PUT(V_INT_YM_COL || ';');
                    WHEN DESCTAB(I).COL_TYPE = 180 THEN
                        DBMS_SQL.COLUMN_VALUE(CUR_ID, I, V_TS_COL);      -- Atribui valor a variavel
                        UTL_FILE.PUT(W_FILE, V_TS_COL || ';');
                       -- DBMS_OUTPUT.PUT(V_TS_COL || ';');
                    WHEN DESCTAB(I).COL_TYPE = 181 THEN
                        DBMS_SQL.COLUMN_VALUE(CUR_ID, I, V_TSTZ_COL);    -- Atribui valor a variavel
                        UTL_FILE.PUT(W_FILE, V_TSTZ_COL || ';');
                       -- DBMS_OUTPUT.PUT(V_TSTZ_COL || ';');
                    WHEN DESCTAB(I).COL_TYPE = 231 THEN
                        DBMS_SQL.COLUMN_VALUE(CUR_ID, I, V_TSLTZ_COL);   -- Atribui valor a variavel
                        UTL_FILE.PUT(W_FILE, V_TSLTZ_COL || ';');
                       -- DBMS_OUTPUT.PUT(V_TSLTZ_COL || ';');
                    WHEN DESCTAB(I).COL_TYPE IN (1,96,11,208) THEN
                        DBMS_SQL.COLUMN_VALUE(CUR_ID, I, V_VCH_COL);     -- Atribui valor a variavel
                        UTL_FILE.PUT(W_FILE, V_VCH_COL || ';');
                       -- DBMS_OUTPUT.PUT(V_VCH_COL || ';');
                    ELSE
                        NULL;
                END CASE;
            END LOOP;  -- FOR -- Percorre os campos


            UTL_FILE.NEW_LINE(W_FILE);
           -- DBMS_OUTPUT.NEW_LINE();

        END LOOP;   -- WHILE  -- Todos os registros

        -- Fecha o Cursor
        DBMS_SQL.CLOSE_CURSOR(CUR_ID);

    EXCEPTION
        WHEN OTHERS THEN
            IF DBMS_SQL.IS_OPEN(CUR_ID) THEN
              DBMS_SQL.CLOSE_CURSOR(CUR_ID);
            END IF;
            IF UTL_FILE.IS_OPEN(W_FILE) THEN
               UTL_FILE.FCLOSE(W_FILE);
            END IF;
    END;

    -- FECHA CURSOR
    IF DBMS_SQL.IS_OPEN(CUR_ID) THEN
       DBMS_SQL.CLOSE_CURSOR(CUR_ID);
    END IF;

    -- FECHA ARQUIVO
    IF UTL_FILE.IS_OPEN(W_FILE) THEN
       UTL_FILE.FCLOSE(W_FILE);
    END IF;

     -- DBMS_OUTPUT.PUT_LINE(CRLF || 'Arquivo criado em: ' || P_PATH_DIR || P_FILE_NAME );

END;
/

Show errors;
/

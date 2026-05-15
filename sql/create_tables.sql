-- =============================================================
-- ETL SII Bienes Raíces - Esquema MySQL
-- Base de datos: sii_bienes_raices
-- Versión: 2.0  |  Mayo 2026
-- Mejora: tablas staging para carga diferencial (solo nuevos/modificados)
-- =============================================================

CREATE DATABASE IF NOT EXISTS sii_bienes_raices
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE sii_bienes_raices;

-- -------------------------------------------------------------
-- CATÁLOGO DE COMUNAS
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS comunas (
    codigo_sii      VARCHAR(10)  NOT NULL,
    nombre          VARCHAR(100) NOT NULL,
    region          VARCHAR(5)   NULL,
    created_at      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (codigo_sii)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO comunas (codigo_sii, nombre, region) VALUES
    ('14111', 'Pudahuel',   'RM'),
    ('14166', 'Cerrillos',  'RM'),
    ('13101', 'Santiago',   'RM'),
    ('14109', 'Maipú',      'RM'),
    ('16301', 'Puente Alto','RM'),
    ('15128', 'La Florida', 'RM'),
    ('15108', 'Las Condes', 'RM');

-- -------------------------------------------------------------
-- TABLAS DE PRODUCCIÓN
-- row_hash: MD5 de los campos de negocio para detectar cambios
-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS rol_cobro (
    id                      BIGINT        NOT NULL AUTO_INCREMENT,
    codigo_comuna           VARCHAR(10)   NOT NULL,
    anio                    SMALLINT      NOT NULL,
    semestre                TINYINT       NOT NULL,
    indicador_aseo          CHAR(1)       NULL COMMENT 'A=incluye aseo',
    direccion_predial       VARCHAR(40)   NULL,
    manzana                 VARCHAR(10)   NOT NULL,
    predio                  VARCHAR(10)   NOT NULL,
    codigo_serie            CHAR(1)       NULL COMMENT 'A=Agricola N=No Agricola',
    cuota_trimestral        DECIMAL(13,2) NULL,
    avaluo_total            DECIMAL(15,2) NULL,
    avaluo_exento           DECIMAL(15,2) NULL,
    anio_termino_exencion   SMALLINT      NULL COMMENT '2055=indefinida',
    codigo_ubicacion        CHAR(1)       NULL COMMENT 'R=Rural U=Urbana',
    codigo_destino          CHAR(1)       NULL,
    fuente_archivo          VARCHAR(150)  NULL,
    row_hash                CHAR(32)      NULL COMMENT 'MD5 para detección de cambios',
    fecha_carga             TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_rol_cobro (codigo_comuna, manzana, predio, anio, semestre),
    KEY idx_rc_comuna       (codigo_comuna),
    KEY idx_rc_periodo      (anio, semestre),
    KEY idx_rc_destino      (codigo_destino)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS roles_agricolas (
    id                          BIGINT        NOT NULL AUTO_INCREMENT,
    codigo_comuna               VARCHAR(10)   NOT NULL,
    anio                        SMALLINT      NOT NULL,
    semestre                    TINYINT       NOT NULL,
    numero_manzana              VARCHAR(10)   NOT NULL,
    numero_predial              VARCHAR(10)   NOT NULL,
    direccion_predio            VARCHAR(200)  NULL,
    avaluo_fiscal_total         DECIMAL(15,2) NULL,
    contribucion_semestral      DECIMAL(15,2) NULL,
    codigo_destino_principal    CHAR(1)       NULL,
    avaluo_exento               DECIMAL(15,2) NULL,
    codigo_ubicacion            CHAR(1)       NULL COMMENT 'R=Rural U=Urbana',
    fuente_archivo              VARCHAR(150)  NULL,
    row_hash                    CHAR(32)      NULL,
    fecha_carga                 TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_roles_agri (codigo_comuna, numero_manzana, numero_predial, anio, semestre),
    KEY idx_agri_comuna         (codigo_comuna),
    KEY idx_agri_periodo        (anio, semestre),
    KEY idx_agri_destino        (codigo_destino_principal)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS detalle_agricola (
    id                          BIGINT        NOT NULL AUTO_INCREMENT,
    codigo_comuna               VARCHAR(10)   NOT NULL,
    anio                        SMALLINT      NOT NULL,
    semestre                    TINYINT       NOT NULL,
    numero_manzana              VARCHAR(10)   NOT NULL,
    numero_predial              VARCHAR(10)   NOT NULL,
    codigo_suelo                VARCHAR(5)    NULL COMMENT '1R,2R,1-8 clase secano',
    superficie_suelo            DECIMAL(12,2) NULL COMMENT 'En hectáreas (raw/100)',
    num_linea_construccion      SMALLINT      NULL,
    codigo_material             VARCHAR(5)    NULL,
    codigo_calidad              CHAR(1)       NULL COMMENT '1=Superior 5=Inferior',
    superficie_construccion     INT           NULL COMMENT 'En m2 sin decimales',
    codigo_destino              CHAR(1)       NULL,
    codigo_condicion_especial   VARCHAR(5)    NULL,
    numero_pisos                TINYINT       NULL,
    fuente_archivo              VARCHAR(150)  NULL,
    row_hash                    CHAR(32)      NULL,
    fecha_carga                 TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_detalle_agri (codigo_comuna, numero_manzana, numero_predial,
                                anio, semestre, codigo_suelo, num_linea_construccion),
    KEY idx_da_comuna   (codigo_comuna),
    KEY idx_da_rol      (codigo_comuna, numero_manzana, numero_predial, anio, semestre)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS roles_no_agricolas (
    id                          BIGINT        NOT NULL AUTO_INCREMENT,
    codigo_comuna               VARCHAR(10)   NOT NULL,
    anio                        SMALLINT      NOT NULL,
    semestre                    TINYINT       NOT NULL,
    numero_manzana              VARCHAR(10)   NOT NULL,
    numero_predial              VARCHAR(10)   NOT NULL,
    direccion_predio            VARCHAR(200)  NULL,
    avaluo_fiscal_total         DECIMAL(15,2) NULL,
    contribucion_semestral      DECIMAL(15,2) NULL,
    codigo_destino_principal    CHAR(1)       NULL,
    avaluo_exento               DECIMAL(15,2) NULL,
    codigo_comuna_bc1           VARCHAR(10)   NULL COMMENT 'Bien Común 1',
    num_manzana_bc1             VARCHAR(10)   NULL,
    num_predio_bc1              VARCHAR(10)   NULL,
    codigo_comuna_bc2           VARCHAR(10)   NULL COMMENT 'Bien Común 2',
    num_manzana_bc2             VARCHAR(10)   NULL,
    num_predio_bc2              VARCHAR(10)   NULL,
    superficie_total_terreno    INT           NULL COMMENT 'En m2 sin decimales',
    codigo_ubicacion            CHAR(1)       NULL COMMENT 'R=Rural U=Urbana',
    codigo_comuna_padre         VARCHAR(10)   NULL,
    num_manzana_padre           VARCHAR(10)   NULL,
    num_predio_padre            VARCHAR(10)   NULL,
    fuente_archivo              VARCHAR(150)  NULL,
    row_hash                    CHAR(32)      NULL,
    fecha_carga                 TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_roles_noagri (codigo_comuna, numero_manzana, numero_predial, anio, semestre),
    KEY idx_na_comuna           (codigo_comuna),
    KEY idx_na_periodo          (anio, semestre),
    KEY idx_na_destino          (codigo_destino_principal),
    KEY idx_na_ubicacion        (codigo_ubicacion)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS detalle_no_agricola (
    id                          BIGINT        NOT NULL AUTO_INCREMENT,
    codigo_comuna               VARCHAR(10)   NOT NULL,
    anio                        SMALLINT      NOT NULL,
    semestre                    TINYINT       NOT NULL,
    numero_manzana              VARCHAR(10)   NOT NULL,
    numero_predial              VARCHAR(10)   NOT NULL,
    num_linea_construccion      SMALLINT      NULL,
    codigo_material             VARCHAR(5)    NULL,
    codigo_calidad              CHAR(1)       NULL COMMENT '1=Superior 5=Inferior',
    anio_construccion           SMALLINT      NULL,
    superficie_construccion     INT           NULL COMMENT 'En m2 sin decimales',
    codigo_destino              CHAR(1)       NULL,
    codigo_condicion_especial   VARCHAR(5)    NULL COMMENT 'AL,CA,CI,MS,PZ,SB,TM',
    numero_pisos                TINYINT       NULL,
    fuente_archivo              VARCHAR(150)  NULL,
    row_hash                    CHAR(32)      NULL,
    fecha_carga                 TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_detalle_noagri (codigo_comuna, numero_manzana, numero_predial,
                                  anio, semestre, num_linea_construccion),
    KEY idx_dna_comuna  (codigo_comuna),
    KEY idx_dna_rol     (codigo_comuna, numero_manzana, numero_predial, anio, semestre),
    KEY idx_dna_anio    (anio_construccion)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- -------------------------------------------------------------
-- LOG DE CARGAS
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS carga_log (
    id                      INT           NOT NULL AUTO_INCREMENT,
    nombre_archivo          VARCHAR(200)  NOT NULL,
    tipo_archivo            VARCHAR(50)   NOT NULL,
    codigo_comuna           VARCHAR(10)   NULL,
    anio                    SMALLINT      NULL,
    semestre                TINYINT       NULL,
    es_nacional             TINYINT(1)    DEFAULT 0,
    registros_leidos        INT           NULL,
    registros_insertados    INT           NULL,
    registros_actualizados  INT           NULL COMMENT 'Registros existentes con cambios',
    registros_ignorados     INT           NULL COMMENT 'Sin cambios respecto a BD',
    estado                  ENUM('INICIADO','COMPLETADO','ERROR') DEFAULT 'INICIADO',
    mensaje_error           TEXT          NULL,
    inicio_carga            DATETIME      NULL,
    fin_carga               DATETIME      NULL,
    PRIMARY KEY (id),
    KEY idx_log_archivo     (nombre_archivo),
    KEY idx_log_estado      (estado)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =============================================================
-- TABLAS STAGING (una por cada tabla de producción)
-- Sin id ni fecha_carga — mismas columnas de negocio + row_hash
-- Se truncan antes de cada carga de archivo
-- =============================================================

CREATE TABLE IF NOT EXISTS stg_rol_cobro (
    codigo_comuna           VARCHAR(10)   NULL,
    anio                    SMALLINT      NULL,
    semestre                TINYINT       NULL,
    indicador_aseo          CHAR(1)       NULL,
    direccion_predial       VARCHAR(40)   NULL,
    manzana                 VARCHAR(10)   NULL,
    predio                  VARCHAR(10)   NULL,
    codigo_serie            CHAR(1)       NULL,
    cuota_trimestral        DECIMAL(13,2) NULL,
    avaluo_total            DECIMAL(15,2) NULL,
    avaluo_exento           DECIMAL(15,2) NULL,
    anio_termino_exencion   SMALLINT      NULL,
    codigo_ubicacion        CHAR(1)       NULL,
    codigo_destino          CHAR(1)       NULL,
    fuente_archivo          VARCHAR(150)  NULL,
    row_hash                CHAR(32)      NULL,
    KEY idx_stg_rc (codigo_comuna, manzana, predio, anio, semestre)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS stg_roles_agricolas (
    codigo_comuna               VARCHAR(10)   NULL,
    anio                        SMALLINT      NULL,
    semestre                    TINYINT       NULL,
    numero_manzana              VARCHAR(10)   NULL,
    numero_predial              VARCHAR(10)   NULL,
    direccion_predio            VARCHAR(200)  NULL,
    avaluo_fiscal_total         DECIMAL(15,2) NULL,
    contribucion_semestral      DECIMAL(15,2) NULL,
    codigo_destino_principal    CHAR(1)       NULL,
    avaluo_exento               DECIMAL(15,2) NULL,
    codigo_ubicacion            CHAR(1)       NULL,
    fuente_archivo              VARCHAR(150)  NULL,
    row_hash                    CHAR(32)      NULL,
    KEY idx_stg_ra (codigo_comuna, numero_manzana, numero_predial, anio, semestre)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS stg_detalle_agricola (
    codigo_comuna               VARCHAR(10)   NULL,
    anio                        SMALLINT      NULL,
    semestre                    TINYINT       NULL,
    numero_manzana              VARCHAR(10)   NULL,
    numero_predial              VARCHAR(10)   NULL,
    codigo_suelo                VARCHAR(5)    NULL,
    superficie_suelo            DECIMAL(12,2) NULL,
    num_linea_construccion      SMALLINT      NULL,
    codigo_material             VARCHAR(5)    NULL,
    codigo_calidad              CHAR(1)       NULL,
    superficie_construccion     INT           NULL,
    codigo_destino              CHAR(1)       NULL,
    codigo_condicion_especial   VARCHAR(5)    NULL,
    numero_pisos                TINYINT       NULL,
    fuente_archivo              VARCHAR(150)  NULL,
    row_hash                    CHAR(32)      NULL,
    KEY idx_stg_da (codigo_comuna, numero_manzana, numero_predial, anio, semestre)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS stg_roles_no_agricolas (
    codigo_comuna               VARCHAR(10)   NULL,
    anio                        SMALLINT      NULL,
    semestre                    TINYINT       NULL,
    numero_manzana              VARCHAR(10)   NULL,
    numero_predial              VARCHAR(10)   NULL,
    direccion_predio            VARCHAR(200)  NULL,
    avaluo_fiscal_total         DECIMAL(15,2) NULL,
    contribucion_semestral      DECIMAL(15,2) NULL,
    codigo_destino_principal    CHAR(1)       NULL,
    avaluo_exento               DECIMAL(15,2) NULL,
    codigo_comuna_bc1           VARCHAR(10)   NULL,
    num_manzana_bc1             VARCHAR(10)   NULL,
    num_predio_bc1              VARCHAR(10)   NULL,
    codigo_comuna_bc2           VARCHAR(10)   NULL,
    num_manzana_bc2             VARCHAR(10)   NULL,
    num_predio_bc2              VARCHAR(10)   NULL,
    superficie_total_terreno    INT           NULL,
    codigo_ubicacion            CHAR(1)       NULL,
    codigo_comuna_padre         VARCHAR(10)   NULL,
    num_manzana_padre           VARCHAR(10)   NULL,
    num_predio_padre            VARCHAR(10)   NULL,
    fuente_archivo              VARCHAR(150)  NULL,
    row_hash                    CHAR(32)      NULL,
    KEY idx_stg_na (codigo_comuna, numero_manzana, numero_predial, anio, semestre)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS stg_detalle_no_agricola (
    codigo_comuna               VARCHAR(10)   NULL,
    anio                        SMALLINT      NULL,
    semestre                    TINYINT       NULL,
    numero_manzana              VARCHAR(10)   NULL,
    numero_predial              VARCHAR(10)   NULL,
    num_linea_construccion      SMALLINT      NULL,
    codigo_material             VARCHAR(5)    NULL,
    codigo_calidad              CHAR(1)       NULL,
    anio_construccion           SMALLINT      NULL,
    superficie_construccion     INT           NULL,
    codigo_destino              CHAR(1)       NULL,
    codigo_condicion_especial   VARCHAR(5)    NULL,
    numero_pisos                TINYINT       NULL,
    fuente_archivo              VARCHAR(150)  NULL,
    row_hash                    CHAR(32)      NULL,
    KEY idx_stg_dna (codigo_comuna, numero_manzana, numero_predial, anio, semestre)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =============================================================
-- UPGRADE v1.0 → v2.0 (ejecutar si ya existe la BD sin staging)
-- MySQL 8.0+ soporta ADD COLUMN IF NOT EXISTS
-- =============================================================

ALTER TABLE rol_cobro
    ADD COLUMN IF NOT EXISTS row_hash CHAR(32) NULL AFTER fuente_archivo;

ALTER TABLE roles_agricolas
    ADD COLUMN IF NOT EXISTS row_hash CHAR(32) NULL AFTER fuente_archivo;

ALTER TABLE detalle_agricola
    ADD COLUMN IF NOT EXISTS row_hash CHAR(32) NULL AFTER fuente_archivo,
    ADD UNIQUE KEY IF NOT EXISTS uk_detalle_agri
        (codigo_comuna, numero_manzana, numero_predial, anio, semestre,
         codigo_suelo, num_linea_construccion);

ALTER TABLE roles_no_agricolas
    ADD COLUMN IF NOT EXISTS row_hash CHAR(32) NULL AFTER fuente_archivo;

ALTER TABLE detalle_no_agricola
    ADD COLUMN IF NOT EXISTS row_hash CHAR(32) NULL AFTER fuente_archivo,
    ADD UNIQUE KEY IF NOT EXISTS uk_detalle_noagri
        (codigo_comuna, numero_manzana, numero_predial, anio, semestre,
         num_linea_construccion);

ALTER TABLE carga_log
    ADD COLUMN IF NOT EXISTS registros_actualizados INT NULL
        AFTER registros_insertados;

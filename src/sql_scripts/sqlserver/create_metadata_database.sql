
CREATE TABLE [dbo].[columns](
	[SERVER_NAME] [varchar](255) NULL,
	[TABLE_CATALOG] [varchar](255) NULL,
	[TABLE_SCHEMA] [varchar](255) NULL,
	[TABLE_NAME] [varchar](255) NULL,
	[COLUMN_NAME] [varchar](255) NULL,
	[ORDINAL_POSITION] [int] NULL,
	[DATA_TYPE] [varchar](255) NULL
)

CREATE UNIQUE INDEX idx_columns ON columns ([SERVER_NAME], [TABLE_CATALOG], [TABLE_NAME], [COLUMN_NAME]);

CREATE TABLE [dbo].[tables](
	[SERVER_NAME] [varchar](255) NULL,
	[TABLE_CATALOG] [varchar](255) NULL,
	[TABLE_SCHEMA] [varchar](255) NULL,
	[TABLE_NAME] [varchar](255) NULL,
	[N_COLUMNS] [int] NULL,
	[N_ROWS] [int] NULL
)

CREATE TABLE [dbo].[uniques](
	[SERVER_NAME] [varchar](255) NULL,
	[TABLE_CATALOG] [varchar](255) NULL,
	[TABLE_SCHEMA] [varchar](255) NULL,
	[TABLE_NAME] [varchar](255) NULL,
	[COLUMN_NAME] [varchar](255) NULL,
	[ORDINAL_POSITION] [int] NULL,
	[DATA_TYPE] [varchar](255) NULL,
	[DISTINCT_VALUES] [int] NULL,
	[NULL_VALUES] [int] NULL
)

CREATE TABLE [dbo].[data_values](
	[SERVER_NAME] [varchar](255) NULL,
	[TABLE_CATALOG] [varchar](255) NULL,
	[TABLE_SCHEMA] [varchar](255) NULL,
	[TABLE_NAME] [varchar](255) NULL,
	[COLUMN_NAME] [varchar](255) NULL,
	[DATA_VALUE] [varchar](255) NULL,
	[FREQUENCY_NUMBER] [int] NULL,
	[FREQUENCY_PERCENTAGE] [float] NULL
)

CREATE INDEX idx_t_c_d ON data_values ([TABLE_NAME], [COLUMN_NAME], [DATA_VALUE]);

CREATE TABLE [dbo].[dates](
	[SERVER_NAME] [varchar](255) NULL,
	[TABLE_CATALOG] [varchar](255) NULL,
	[TABLE_SCHEMA] [varchar](255) NULL,
	[TABLE_NAME] [varchar](255) NULL,
	[COLUMN_NAME] [varchar](255) NULL,
	[DATA_VALUE] [varchar](255) NULL,
	[FREQUENCY_NUMBER] [int] NULL,
	[FREQUENCY_PERCENTAGE] [float] NULL
)

CREATE TABLE [dbo].[stats](
	[SERVER_NAME] [varchar](255) NULL,
	[TABLE_CATALOG] [varchar](255) NULL,
	[TABLE_SCHEMA] [varchar](255) NULL,
	[TABLE_NAME] [varchar](255) NULL,
	[COLUMN_NAME] [varchar](255) NULL,
	[AVG] [float] NULL,
	[STDEV] [float] NULL,
	[VAR] [float] NULL,
	[SUM] [float] NULL,
	[MAX] [float] NULL,
	[MIN] [float] NULL,
	[RANGE_] [float] NULL,
	[P01] [float] NULL,
	[P025] [float] NULL,
	[P05] [float] NULL,
	[P10] [float] NULL,
	[Q1] [float] NULL,
	[Q2] [float] NULL,
	[Q3] [float] NULL,
	[P90] [float] NULL,
	[P95] [float] NULL,
	[P975] [float] NULL,
	[P99] [float] NULL,
	[IQR] [float] NULL
)
CREATE TABLE public.stagingLegislatures (
	legislatureId INT,
    uri VARCHAR(256),
	startDate DATE,
	endDate DATE,
	electionYear INT
);

CREATE TABLE public.stagingDeputies (
	uri VARCHAR(256),
	name VARCHAR(256),
	firstLegislatureId INT,
	lastLegislatureId INT,
	civilName VARCHAR(256),
	gender VARCHAR(1),
	birthDate DATE,
	deathDate DATE,
	birthState VARCHAR(2),
	birthCounty VARCHAR(128)
);

CREATE TABLE public.stagingDeputiesDetails (
	deputyId VARCHAR(8),
	deputyDocumentId VARCHAR(16),
	party VARCHAR(32),
	electionState VARCHAR(2),
	schoolLevel VARCHAR(32)
);

CREATE TABLE public.stagingExpenses (
	name VARCHAR(256),
	deputyDocumentId VARCHAR(16),
	registerId VARCHAR(16),
	deputyDocumentNu VARCHAR(16),
	legislatureNu INT,
	stateCode VARCHAR(2),
	party VARCHAR(128),
	legislatureId INT,
	subQuotaNu VARCHAR(16),
	description VARCHAR(256),
	subQuotaSpecificationNu VARCHAR(16),
	specificationDescription VARCHAR(256),
	provider VARCHAR(256),
	providerDocumentId VARCHAR(20),
	number VARCHAR(32),
	documentType VARCHAR(16),
	emissionDate TIMESTAMP,
	documentValue DECIMAL(9, 2),
	glValue DECIMAL(9, 2),
	liqValue DECIMAL(9, 2),
	month INT,
	year INT,
	parcelNu VARCHAR(4),
	passenger VARCHAR(256),
	flight VARCHAR(256),
	batchNu INT,
	refundNu VARCHAR(16),
	restitutionNu VARCHAR(16),
	deputyId INT,
	documentId INT,
	documentUrl VARCHAR(256)
);

CREATE TABLE public.legislatures (
	legislatureId INT PRIMARY KEY NOT NULL,
	electionYear INT
);

CREATE TABLE public.newDeputies (
	deputyId INT PRIMARY KEY NOT NULL,
	uri VARCHAR(256)
);

CREATE TABLE public.parties (
	partyId INT PRIMARY KEY IDENTITY(1, 1),
	party VARCHAR(32)
);

CREATE TABLE public.states (
	stateId INT PRIMARY KEY IDENTITY(1, 1),
	stateAbb VARCHAR(2)
);

CREATE TABLE public.schoolLevels (
	schoolLevelId INT PRIMARY KEY IDENTITY(1, 1),
	schoolLevel VARCHAR(32)
);

CREATE TABLE public.deputies (
	deputyId INT PRIMARY KEY NOT NULL,
	name VARCHAR(256) NOT NULL,
	civilName VARCHAR(256) NOT NULL,
	gender VARCHAR(1),
	birthDate DATE,
	deathDate DATE,
	deputyDocumentId VARCHAR(16),
	partyId INT,
	electionStateId INT,
	schoolLevelId INT
);

CREATE TABLE public.expenseTypes (
	expenseTypeId INT PRIMARY KEY IDENTITY(1, 1),
	expenseTypeDescription VARCHAR(256)
);

CREATE TABLE public.expenses (
	expenseId INT PRIMARY KEY IDENTITY(1, 1),
	legislatureId INT NOT NULL,
	deputyId INT,
	expenseTypeId INT,
	emissionDate DATE,
	provider VARCHAR(256),
	providerDocumentId VARCHAR(20),
	month INT,
	year INT,
	documentValue DECIMAL(9, 2),
	glValue DECIMAL(9, 2),
	liqValue DECIMAL(9, 2),
	parcelNu INT,
	documentId INT,
	documentUrl VARCHAR(256)
);

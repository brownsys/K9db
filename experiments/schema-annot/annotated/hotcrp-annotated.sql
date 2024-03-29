-- not supported by k9db:
-- col types varbinary, blob, varchar
-- multi column primary keys
-- auto increment
-- default values
-- KEY / UNIQUE KEY

CREATE DATA_SUBJECT TABLE ContactInfo (
  contactId int NOT NULL,
  firstName text NOT NULL,
  lastName text NOT NULL,
  unaccentedName text NOT NULL,
  email text NOT NULL,
  preferredEmail text,
  affiliation text NOT NULL,
  phone text,
  country text,
  password text NOT NULL,
  passwordTime int NOT NULL,
  passwordUseTime int NOT NULL,
  collaborators text,
  creationTime int NOT NULL,
  updateTime int NOT NULL,
  lastLogin int NOT NULL,
  defaultWatch int NOT NULL,
  roles int NOT NULL,
  disabled int NOT NULL,
  contactTags text,
  birthday int,
  gender text,
  data text,
  PRIMARY KEY (contactId)
  -- UNIQUE KEY email (email),
  -- KEY roles (roles)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE PaperStorage (
  paperId int NOT NULL,
  paperStorageId int NOT NULL,
  timestamp int NOT NULL,
  mimetype text NOT NULL,
  paper text,
  compression int NOT NULL,
  sha1 text NOT NULL,
  crc32 text,
  documentType int NOT NULL,
  filename text,
  infoJson text,
  size int,
  filterType int,
  originalStorageId int,
  inactive int NOT NULL,
  PRIMARY KEY (paperStorageId),
  FOREIGN KEY (originalStorageId) REFERENCES PaperStorage(paperStorageId)
  -- UNIQUE KEY paperStorageId (paperStorageId),
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE Paper (
  paperId int NOT NULL,
  title text,
  authorInformation text,
  abstract text,
  collaborators text,
  timeSubmitted int NOT NULL,
  timeWithdrawn int NOT NULL,
  timeFinalSubmitted int NOT NULL,
  timeModified int NOT NULL,
  paperStorageId int NOT NULL,
  -- # sha1 copied from PaperStorage to reduce joins
  sha1 text NOT NULL,
  finalPaperStorageId int NOT NULL,
  blind int NOT NULL,
  outcome int NOT NULL,
  leadContactId int NOT NULL,
  shepherdContactId int NOT NULL,
  managerContactId int NOT NULL,
  capVersion int NOT NULL,
  -- # next 3 fields copied from PaperStorage to reduce joins
  size int NOT NULL,
  mimetype text NOT NULL,
  timestamp int NOT NULL,
  pdfFormatStatus int NOT NULL,
  withdrawReason text,
  paperFormat int,
  dataOverflow text,
  PRIMARY KEY (paperId),
  -- KEY timeSubmitted (timeSubmitted),
  -- KEY leadContactId (leadContactId),
  -- KEY shepherdContactId (shepherdContactId),
  FOREIGN KEY (paperStorageId) REFERENCES PaperStorage(paperStorageId),
  FOREIGN KEY (finalPaperStorageId) REFERENCES PaperStorage(paperStorageId),
  FOREIGN KEY (leadContactId) REFERENCES ONLY ContactInfo(contactId),
  FOREIGN KEY (shepherdContactId) ACCESSED_BY ContactInfo(contactId),
  FOREIGN KEY (managerContactId) ACCESSED_BY ContactInfo(contactId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-- Added this table to work around issue #172.
-- Extracts co-author conflictType from PaperConflict
CREATE TABLE PaperAuthors (
  id int PRIMARY KEY NOT NULL,
  paperId int NOT NULL,
  contactId int NOT NULL,
  FOREIGN KEY (paperId) OWNS Paper(paperId),
  FOREIGN KEY (contactId) OWNED_BY ContactInfo(contactId)
)

CREATE TABLE PaperConflict (
  id int PRIMARY KEY NOT NULL,
  paperId int NOT NULL,
  contactId int NOT NULL,
  conflictType int NOT NULL,
  FOREIGN KEY (paperId) ACCESSED_BY Paper(paperId),
  -- see issue #172: ownership/edge in data ownership graph dependent on column value of conflictType,
  -- which could be co-author, institutional, etc.
  FOREIGN KEY (contactId) OWNED_BY ContactInfo(contactId),
  ON DEL (paperId) DELETE_ROW
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE ActionLog (
  logId int NOT NULL,
  contactId int NOT NULL,
  destContactId int,
  trueContactId int,
  paperId int,
  timestamp datetime NOT NULL,
  ipaddr text,
  -- this column name causes k9db to crash with "no viable alternative at input ',\n action'"
  -- action text NOT NULL,
  data text,
  PRIMARY KEY (logId),
  FOREIGN KEY (contactId) OWNED_BY ContactInfo(contactId),
  FOREIGN KEY (destContactId) ACCESSED_BY ContactInfo(contactId),
  FOREIGN KEY (trueContactId) ACCESSED_BY ContactInfo(contactId),
  FOREIGN KEY (paperId) REFERENCES Paper(paperId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE Capability (
  capabilityType int NOT NULL,
  contactId int NOT NULL,
  paperId int NOT NULL,
  timeExpires int NOT NULL,
  salt text NOT NULL,
  data text,
  PRIMARY KEY (salt),
  FOREIGN KEY (contactId) OWNED_BY ContactInfo(contactId),
  FOREIGN KEY (paperId) REFERENCES Paper(paperId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE DeletedContactInfo (
  contactId int NOT NULL,
  firstName text NOT NULL,
  lastName text NOT NULL,
  unaccentedName text NOT NULL,
  email text NOT NULL,
  PRIMARY KEY (contactId),
  FOREIGN KEY (contactId) OWNED_BY ContactInfo(contactId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE DocumentLink (
  paperId int NOT NULL,
  linkId int NOT NULL,
  linkType int NOT NULL,
  documentId int NOT NULL,
  PRIMARY KEY (documentId),
  FOREIGN KEY (paperId) REFERENCES Paper(paperId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE FilteredDocument (
  inDocId int NOT NULL,
  filterType int NOT NULL,
  outDocId int NOT NULL,
  createdAt int NOT NULL,
  PRIMARY KEY (inDocId),
  FOREIGN KEY (inDocId) OWNED_BY DocumentLink(documentId),
  FOREIGN KEY (outDocId) REFERENCES DocumentLink(documentId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE Formula (
  formulaId int NOT NULL,
  name text NOT NULL,
  expression text NOT NULL,
  createdBy int NOT NULL,
  timeModified int NOT NULL,
  PRIMARY KEY (formulaId),
  FOREIGN KEY (createdBy) OWNED_BY ContactInfo(contactId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE MailLog (
  mailId int NOT NULL,
  recipients text NOT NULL,
  q text,
  t text,
  paperIds text,
  cc text,
  replyto text,
  subject text,
  emailBody text,
  fromNonChair int NOT NULL,
  status int NOT NULL,
  PRIMARY KEY (mailId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE PaperComment (
  id int PRIMARY KEY NOT NULL,
  paperId int NOT NULL, -- should be OWNER if we could filter on comment type
  commentId int NOT NULL,
  contactId int NOT NULL,
  timeModified int NOT NULL,
  timeNotified int NOT NULL,
  timeDisplayed int NOT NULL,
  comment text,
  commentType int NOT NULL,
  replyTo int NOT NULL,
  ordinal int NOT NULL,
  authorOrdinal int NOT NULL,
  commentTags text,
  commentRound int NOT NULL,
  commentFormat int,
  commentOverflow text,
  -- UNIQUE KEY commentId (commentId),
  -- KEY contactId (contactId),
  -- KEY timeModifiedContact (timeModified,contactId),
  FOREIGN KEY (paperId) REFERENCES Paper(paperId),
  FOREIGN KEY (contactId) OWNED_BY ContactInfo(contactId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE PaperOption (
  id int PRIMARY KEY NOT NULL,
  paperId int NOT NULL,
  optionId int NOT NULL,
  value int NOT NULL,
  data text,
  dataOverflow text,
  FOREIGN KEY (paperId) REFERENCES Paper(paperId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE PaperReview (
  paperId int NOT NULL,
  reviewId int NOT NULL,
  contactId int NOT NULL,
  reviewToken int NOT NULL,
  reviewType int NOT NULL,
  reviewRound int NOT NULL,
  requestedBy int NOT NULL,
  timeRequested int NOT NULL,
  timeRequestNotified int NOT NULL,
  reviewBlind int NOT NULL,
  reviewModified int NOT NULL,
  reviewAuthorModified int,
  reviewSubmitted int,
  reviewNotified int,
  reviewAuthorNotified int NOT NULL,
  reviewAuthorSeen int,
  reviewOrdinal int NOT NULL,
  reviewViewScore int NOT NULL,
  timeDisplayed int NOT NULL,
  timeApprovalRequested int NOT NULL,
  reviewEditVersion int NOT NULL,
  reviewNeedsSubmit int NOT NULL,
  reviewWordCount int,
  reviewFormat int,
  overAllMerit int NOT NULL,
  reviewerQualification int NOT NULL,
  novelty int NOT NULL,
  technicalMerit int NOT NULL,
  interestToCommunity int NOT NULL,
  longevity int NOT NULL,
  grammar int NOT NULL,
  likelyPresentation int NOT NULL,
  suitableForShort int NOT NULL,
  potential int NOT NULL,
  fixability int NOT NULL,
  tfields text,
  sfields text,
  data text,
  PRIMARY KEY (reviewId),
  -- UNIQUE KEY reviewId (reviewId),
  -- KEY contactId (contactId),
  -- KEY reviewType (reviewType),
  -- KEY reviewRound (reviewRound),
  -- KEY requestedBy (requestedBy),
  FOREIGN KEY (paperId) ACCESSED_BY Paper(paperId),
  FOREIGN KEY (contactId) OWNED_BY ContactInfo(contactId),
  FOREIGN KEY (requestedBy) ACCESSED_BY ContactInfo(contactId),
  -- author should see review, but not who reviewed it
  -- only thing author should see is content of review, and when submitted (dates)
  ON GET paperId ANON (reviewId, contactId, requestedBy, reviewToken, reviewType, reviewRound, reviewBlind, reviewModified, reviewAuthorModified, reviewSubmitted, reviewNotified, reviewAuthorNotified, reviewAuthorSeen, reviewOrdinal)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE PaperReviewPreference (
  id int PRIMARY KEY NOT NULL,
  paperId int NOT NULL,
  contactId int NOT NULL,
  preference int NOT NULL,
  expertise int,
  FOREIGN KEY (paperId) REFERENCES Paper(paperId),
  FOREIGN KEY (contactId) OWNED_BY ContactInfo(contactId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE PaperReviewRefused (
  id int PRIMARY KEY NOT NULL,
  paperId int NOT NULL,
  email text NOT NULL,
  firstName text,
  lastName text,
  affiliation text,
  contactId int NOT NULL,
  requestedBy int NOT NULL,
  timeRequested int,
  refusedBy int,
  timeRefused int,
  reviewType int NOT NULL,
  reviewRound int,
  data text,
  reason text,
  FOREIGN KEY (paperId) REFERENCES Paper(paperId),
  FOREIGN KEY (contactId) OWNED_BY ContactInfo(contactId),
  FOREIGN KEY (requestedBy) ACCESSED_BY ContactInfo(contactId),
  FOREIGN KEY (refusedBy) ACCESSED_BY ContactInfo(contactId),
  ON GET requestedBy ANON (email, firstName, lastName, affiliation, contactId),
  ON DEL requestedBy ANON (requestedBy),
  ON GET refusedBy ANON (email, firstName, lastName, affiliation, contactId),
  ON DEL refusedBy ANON (refusedBy)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE PaperTag (
  id int PRIMARY KEY NOT NULL,
  paperId int NOT NULL,
  -- # case-insensitive; see TAG_MAXLEN in init.php
  tag text NOT NULL,		
  tagIndex int NOT NULL,
  FOREIGN KEY (paperId) REFERENCES Paper(paperId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE PaperTagAnno (
  -- # case-insensitive; see TAG_MAXLEN in init.php
  tag text NOT NULL,
  annoId int NOT NULL,
  tagIndex int NOT NULL,
  heading text,
  annoFormat int,
  infoJson text,
  PRIMARY KEY (annoId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE TopicArea (
  topicId int NOT NULL,
  topicName text,
  PRIMARY KEY (topicId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE PaperTopic (
  id int PRIMARY KEY NOT NULL,
  paperId int NOT NULL,
  topicId int NOT NULL,
  FOREIGN KEY (paperId) OWNED_BY Paper(paperId),
  FOREIGN KEY (topicId) REFERENCES TopicArea(topicId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE PaperWatch (
  id int PRIMARY KEY NOT NULL,
  paperId int NOT NULL,
  contactId int NOT NULL,
  watch int NOT NULL,
  FOREIGN KEY (contactId) OWNED_BY ContactInfo(contactId),
  FOREIGN KEY (paperId) REFERENCES Paper(paperId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE ReviewRating (
  id int PRIMARY KEY NOT NULL,
  paperId int NOT NULL,
  reviewId int NOT NULL,
  contactId int NOT NULL,
  rating int NOT NULL,
  FOREIGN KEY (contactId) OWNED_BY ContactInfo(contactId),
  FOREIGN KEY (paperId) REFERENCES Paper(paperId),
  FOREIGN KEY (reviewId) REFERENCES PaperReview(reviewId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- data subject because this could refer to a person who does not have an account on hotcrp
CREATE DATA_SUBJECT TABLE ReviewRequest (
  id int PRIMARY KEY NOT NULL,
  paperId int NOT NULL,
  email text NOT NULL,
  firstName text,
  lastName text,
  affiliation text,
  reason text,
  requestedBy int NOT NULL,
  timeRequested int NOT NULL,
  reviewRound int,
  FOREIGN KEY (paperId) REFERENCES Paper(paperId),
  FOREIGN KEY (requestedBy) ACCESSED_BY ContactInfo(contactId),
  -- authors of papers should not see who reviewed their paper
  ON GET paperId ANON (email, firstName, lastName, affiliation, requestedBy, timeRequested)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE Settings (
  name text NOT NULL,
  value int NOT NULL,
  data text,
  PRIMARY KEY (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE TopicInterest (
  contactId int NOT NULL,
  topicId int NOT NULL,
  interest int NOT NULL,
  PRIMARY KEY (contactId),
  FOREIGN KEY (contactId) REFERENCES ContactInfo(contactId),
  FOREIGN KEY (topicId) REFERENCES TopicArea(topicId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

EXPLAIN COMPLIANCE;

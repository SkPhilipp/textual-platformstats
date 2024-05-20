CREATE TABLE IF NOT EXISTS data_artifactory
(
    _id                   INTEGER PRIMARY KEY,
    _tag                  TEXT DEFAULT 'default',

    ClientAddr            TEXT,
    ClientAddr_ClientIp   TEXT,
    ClientAddr_ClientPort TEXT,
    DownstreamContentSize INTEGER,
    DownstreamStatus      INTEGER,
    Duration              INTEGER,
    RequestMethod         TEXT,
    RequestPath           TEXT,
    ServiceAddr           TEXT,
    StartUTC              TIMESTAMP,
    level                 TEXT,
    msg                   TEXT,
    request_Uber_Trace_Id TEXT,
    request_User_Agent    TEXT,
    time                  TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_tag ON data_artifactory (_tag);
CREATE INDEX IF NOT EXISTS idx_ClientAddr ON data_artifactory (ClientAddr);
CREATE INDEX IF NOT EXISTS idx_ClientAddr_ClientIp ON data_artifactory (ClientAddr_ClientIp);
CREATE INDEX IF NOT EXISTS idx_ClientAddr_ClientPort ON data_artifactory (ClientAddr_ClientPort);
CREATE INDEX IF NOT EXISTS idx_DownstreamContentSize ON data_artifactory (DownstreamContentSize);
CREATE INDEX IF NOT EXISTS idx_DownstreamStatus ON data_artifactory (DownstreamStatus);
CREATE INDEX IF NOT EXISTS idx_Duration ON data_artifactory (Duration);
CREATE INDEX IF NOT EXISTS idx_RequestMethod ON data_artifactory (RequestMethod);
CREATE INDEX IF NOT EXISTS idx_RequestPath ON data_artifactory (RequestPath);
CREATE INDEX IF NOT EXISTS idx_ServiceAddr ON data_artifactory (ServiceAddr);
CREATE INDEX IF NOT EXISTS idx_StartUTC ON data_artifactory (StartUTC);
CREATE INDEX IF NOT EXISTS idx_level ON data_artifactory (level);
CREATE INDEX IF NOT EXISTS idx_msg ON data_artifactory (msg);
CREATE INDEX IF NOT EXISTS idx_request_Uber_Trace_Id ON data_artifactory (request_Uber_Trace_Id);
CREATE INDEX IF NOT EXISTS idx_request_User_Agent ON data_artifactory (request_User_Agent);
CREATE INDEX IF NOT EXISTS idx_time ON data_artifactory (time);

CREATE INDEX IF NOT EXISTS idx_ClientAddr__time ON data_artifactory (ClientAddr, time);

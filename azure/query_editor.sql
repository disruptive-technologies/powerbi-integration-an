CREATE TABLE touch_events (
  event_id char(23) NOT NULL UNIQUE,
  device_id char(20) NOT NULL,
  timestamp char(30) NOT NULL
);
CREATE TABLE temperature_events (
  event_id char(23) NOT NULL UNIQUE,
  device_id char(20) NOT NULL,
  timestamp char(30) NOT NULL,
  temperature float NOT NULL
);
CREATE TABLE prox_events (
  event_id char(23) NOT NULL UNIQUE,
  device_id char(20) NOT NULL,
  timestamp char(30) NOT NULL,
  state varchar(15) NOT NULL
);
CREATE TABLE water_events (
  event_id char(23) NOT NULL UNIQUE,
  device_id char(20) NOT NULL,
  timestamp char(30) NOT NULL,
  state varchar(15) NOT NULL
);
CREATE TABLE humidity_events (
  event_id char(23) NOT NULL UNIQUE,
  device_id char(20) NOT NULL,
  timestamp char(30) NOT NULL,
  temperature float NOT NULL,
  humidity float NOT NULL
);
CREATE TABLE countingprox_events (
  event_id char(23) NOT NULL UNIQUE,
  device_id char(20) NOT NULL,
  timestamp char(30) NOT NULL,
  total integer NOT NULL
);
CREATE TABLE countingtouch_events (
  event_id char(23) NOT NULL UNIQUE,
  device_id char(20) NOT NULL,
  timestamp char(30) NOT NULL,
  total integer NOT NULL
);

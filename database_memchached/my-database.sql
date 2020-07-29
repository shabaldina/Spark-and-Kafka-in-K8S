

CREATE Table `infects` (
    `date` date NOT NUll,
    `cases` int default NULL,
    `deaths` int default NULL,
    `country` varchar(64) NOT NULL,
    `continent` varchar(64) default NULL,
    `cases_prev` int default NULL,
    `dead_prev` int default NULL,
    `cases_rel_diff` decimal(10,4) default NULL,
    `deaths_rel_diff` decimal(10,4) default NULL,
PRIMARY KEY  (`date`, `country`) ) ENGINE=MyISAM DEFAULT CHARSET=latin1;



CREATE Table `dax` (
    `date` date NOT NUll,
    `open` decimal(10,4) default NULL,
    `close` decimal(10,4) default NULL,
    `diff` decimal(10,4) default NULL,
PRIMARY KEY  (`date`) ) ENGINE=MyISAM DEFAULT CHARSET=latin1;
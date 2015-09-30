Darwin-DB
=========

A system for storing Darwin Push Port messages to a PostgreSQL database.

This depends on Darwin Gateway to first transcode the Push Port messages from XML to JSON.

Setup
=====

This code requires *Python 3.4+*.

First make sure you have [Darwin Gateway](https://github.com/fasteroute/darwin-gateway) installed
and running. Then install the dependencies for this:

  $ pip install -r requirements.txt

Then take a look in ```example.py``` and go from there.

Contributing
============

Bug reports and pull requests welcome.



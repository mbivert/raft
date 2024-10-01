/*
 * Tests scenarios:
 *	- single-peer network:
 *		- launch the network
 *		- wait for electiontimeout×2 or something
 *		- our peer should be the self-elected leader
 *	- two-peers network:
 *		- launch the network
 *		- wait for electiontimeout×2 or something
 *		- one peer should be the self-elected leader
 *		- wait a little
 *		- same peer should still be self-elected
 *
 * Then, start to add flaws and more peers.
 */
package main

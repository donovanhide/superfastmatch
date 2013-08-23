Introduction
------------

Superfastmatch is a tool for bulk comparison of text. It enables you to discover the longest common substrings between one document and millions of others.

Requirements
------------

One of:

* 64 bit Linux
* 64 bit Windows
* 64 bit OS-X

and a working install of Mongo DB 2.2 or higher. Details [here](http://docs.mongodb.org/manual/installation/)

Binary Downloads
----------------

* [Linux](https://raw.github.com/donovanhide/superfastmatch-go/master/builds/superfastmatch-linux)
* [Windows](https://raw.github.com/donovanhide/superfastmatch-go/master/builds/superfastmatch.exe)
* [OS X](https://raw.github.com/donovanhide/superfastmatch-go/master/builds/superfastmatch-darwin)

For Linux and OS X you might have to add the execution bit, for example on OS X:

```bash
curl https://raw.github.com/donovanhide/superfastmatch-go/master/builds/superfastmatch-darwin -o superfastmatch
chmod +x ./superfastmatch
```

Example usage
-------------

### Compare the works of Dickens for Biblical influence 

Start superfastmatch in one terminal:

```bash
$ ./superfastmatch
2013/01/27 22:58:56 Started in standalone mode with Hash Width: 24 and Window Size: 30
2013/01/27 22:58:56 Starting Posting Server on: 127.0.0.1:8090
2013/01/27 22:58:56 Starting Queue Processor
2013/01/27 22:58:56 Starting Posting Server on: 127.0.0.1:8091
2013/01/27 22:58:56 Starting API server on: 127.0.0.1:8080
127.0.0.1:8090:2013/01/27 22:58:56 Initialising Posting Server with Window Size: 30 Hash Width: 24 Size: 8388608 Offset: 0
127.0.0.1:8090:2013/01/27 22:58:56 Posting Server Initialised with 0 documents in 0.01 secs Average: 0.00 secs/doc
127.0.0.1:8091:2013/01/27 22:58:56 Initialising Posting Server with Window Size: 30 Hash Width: 24 Size: 8388608 Offset: 8388608
127.0.0.1:8091:2013/01/27 22:58:56 Posting Server Initialised with 0 documents in 0.00 secs Average: 0.00 secs/doc
```

In another load some books by Charles Dickens:

```bash
$ ./superfastmatch add fixtures/gutenberg/Dickens.tar.gz 
Entering:	Dickens.tar.gz
Command: Add Document Target: (1,1)
Added:	Doctype:   1	Docid:   1	Title: bleak_house.txt	QueueId: 5105b40e7b1d26385300000d
Command: Add Document Target: (1,2)
Added:	Doctype:   1	Docid:   2	Title: christmas_carol.txt	QueueId: 5105b40e7b1d26385300000e
Command: Add Document Target: (1,3)
Added:	Doctype:   1	Docid:   3	Title: david_copperfield.txt	QueueId: 5105b40e7b1d26385300000f
Command: Add Document Target: (1,4)
Added:	Doctype:   1	Docid:   4	Title: great_expectations.txt	QueueId: 5105b40e7b1d263853000010
Command: Add Document Target: (1,5)
Added:	Doctype:   1	Docid:   5	Title: hard_times.txt	QueueId: 5105b40e7b1d263853000011
Command: Add Document Target: (1,6)
Added:	Doctype:   1	Docid:   6	Title: little_dorrit.txt	QueueId: 5105b40f7b1d263853000012
Command: Add Document Target: (1,7)
Added:	Doctype:   1	Docid:   7	Title: old_curiosity_shop.txt	QueueId: 5105b40f7b1d263853000013
Command: Add Document Target: (1,8)
Added:	Doctype:   1	Docid:   8	Title: oliver_twist.txt	QueueId: 5105b40f7b1d263853000014
Command: Add Document Target: (1,9)
Added:	Doctype:   1	Docid:   9	Title: pickwick_papers.txt	QueueId: 5105b40f7b1d263853000015
Command: Add Document Target: (1,10)
Added:	Doctype:   1	Docid:  10	Title: tale_of_two_cities.txt	QueueId: 5105b40f7b1d263853000016
Successes: 10	 Failures:0
```

And then search for the bible in this collection of documents:

```bash
$ ./superfastmatch search fixtures/gutenberg/bible.txt.gz 
Doc:bleak_house.txt (1,1)	Left:  3728816	Right:    48538	Length:       67	Text: He that is without sin among you, let him first cast a st...
Doc:bleak_house.txt (1,1)	Left:  3728739	Right:    48461	Length:       55	Text: So when they continued asking him, he lifted up himself
Doc:bleak_house.txt (1,1)	Left:  3532111	Right:    48773	Length:       41	Text: Lest coming suddenly he find you sleeping
Doc:bleak_house.txt (1,1)	Left:  2299962	Right:   561545	Length:       40	Text: enter not into judgment with thy servant
Doc:bleak_house.txt (1,1)	Left:  3608991	Right:  1528396	Length:       34	Text: the labourer is worthy of his hire
Doc:bleak_house.txt (1,1)	Left:    68502	Right:   978540	Length:       33	Text: e up early in the morning, and to
Doc:bleak_house.txt (1,1)	Left:   105106	Right:   978540	Length:       33	Text: e up early in the morning, and to
Doc:bleak_house.txt (1,1)	Left:  1460766	Right:   978540	Length:       32	Text: e up early in the morning, and t
Doc:bleak_house.txt (1,1)	Left:    12745	Right:     7100	Length:       31	Text: from the face of the earth; and
Doc:bleak_house.txt (1,1)	Left:    31014	Right:   360067	Length:       31	Text: a mighty hunter before the LORD
Doc:bleak_house.txt (1,1)	Left:  1199228	Right:   978539	Length:       31	Text: be up early in the morning, and
Doc:bleak_house.txt (1,1)	Left:  3439534	Right:  1249000	Length:       31	Text: marrying and giving in marriage
Doc:bleak_house.txt (1,1)	Left:    71574	Right:   978540	Length:       30	Text: e up early in the morning, and
Doc:bleak_house.txt (1,1)	Left:   235452	Right:   978540	Length:       30	Text: e up early in the morning, and
Doc:bleak_house.txt (1,1)	Left:   239383	Right:   978540	Length:       30	Text: e up early in the morning, and
Doc:bleak_house.txt (1,1)	Left:   302791	Right:   978540	Length:       30	Text: e up early in the morning, and
Doc:bleak_house.txt (1,1)	Left:   350278	Right:   978540	Length:       30	Text: e up early in the morning, and
Doc:bleak_house.txt (1,1)	Left:   593938	Right:   978540	Length:       30	Text: e up early in the morning, and
Doc:bleak_house.txt (1,1)	Left:   876859	Right:   978540	Length:       30	Text: e up early in the morning, and
Doc:bleak_house.txt (1,1)	Left:   880393	Right:   978540	Length:       30	Text: e up early in the morning, and
Doc:bleak_house.txt (1,1)	Left:  1141838	Right:   978540	Length:       30	Text: e up early in the morning, and
Doc:bleak_house.txt (1,1)	Left:  1968978	Right:   978540	Length:       30	Text: e up early in the morning, and
Doc:old_curiosity_s (1,7)	Left:  2378359	Right:  1082552	Length:       32	Text: heap coals of fire upon his head
Doc:old_curiosity_s (1,7)	Left:  1738254	Right:   462753	Length:       31	Text: and married fourteen wives, and
Doc:old_curiosity_s (1,7)	Left:  3684636	Right:   489980	Length:       30	Text: very early in the morning, the
Doc:great_expectati (1,4)	Left:  3651100	Right:   944296	Length:       31	Text: went up into the temple to pray
Doc:pickwick_papers (1,9)	Left:  1942291	Right:  1730970	Length:       37	Text: he had neither father nor mother, and
Doc:pickwick_papers (1,9)	Left:  1139565	Right:   401542	Length:       34	Text: on the other side: and there was a
Doc:pickwick_papers (1,9)	Left:  1838405	Right:    19021	Length:       30	Text: thousand eight hundred and twe
Doc:tale_of_two_cit (1,10)	Left:  3746360	Right:   628647	Length:       63	Text: he that believeth in me, though he were dead, yet shall h...
Doc:tale_of_two_cit (1,10)	Left:  3746360	Right:   630608	Length:       63	Text: he that believeth in me, though he were dead, yet shall h...
Doc:tale_of_two_cit (1,10)	Left:  3746360	Right:   754350	Length:       63	Text: he that believeth in me, though he were dead, yet shall h...
Doc:tale_of_two_cit (1,10)	Left:  3746431	Right:   754415	Length:       56	Text: And whosoever liveth and believeth in me shall never die
Doc:tale_of_two_cit (1,10)	Left:  3746431	Right:   628712	Length:       40	Text: And whosoever liveth and believeth in me
Doc:tale_of_two_cit (1,10)	Left:  3746431	Right:   630673	Length:       40	Text: And whosoever liveth and believeth in me
Doc:tale_of_two_cit (1,10)	Left:  3063584	Right:   527721	Length:       33	Text: the pavement by the side of the g
Doc:tale_of_two_cit (1,10)	Left:    12745	Right:   475309	Length:       31	Text: from the face of the earth; and
Doc:tale_of_two_cit (1,10)	Left:   342333	Right:   475304	Length:       31	Text: them from the face of the earth
Doc:tale_of_two_cit (1,10)	Left:  3528147	Right:   667929	Length:       30	Text: when all these things shall be
Doc:little_dorrit.t (1,6)	Left:  3608991	Right:  1414177	Length:       34	Text: the labourer is worthy of his hire
Doc:little_dorrit.t (1,6)	Left:  3549670	Right:  1693887	Length:       32	Text: According to the custom of the p
Doc:little_dorrit.t (1,6)	Left:  2479377	Right:   149352	Length:       31	Text: n from generation to generation
Doc:little_dorrit.t (1,6)	Left:  2597837	Right:   149352	Length:       31	Text: n from generation to generation
Doc:little_dorrit.t (1,6)	Left:  2856706	Right:   149352	Length:       31	Text: n from generation to generation
Doc:little_dorrit.t (1,6)	Left:  1838405	Right:   501606	Length:       30	Text: thousand eight hundred and twe
Doc:little_dorrit.t (1,6)	Left:  3140105	Right:   937056	Length:       30	Text: very early in the morning, and
Doc:little_dorrit.t (1,6)	Left:  3716920	Right:   467952	Length:       30	Text: stood on the other side of the
Doc:christmas_carol (1,2)	Left:  3508953	Right:   137229	Length:       53	Text: And he took a child, and set him in the midst of them
Doc:christmas_carol (1,2)	Left:  1533926	Right:    99706	Length:       32	Text: and their children's children: a
Doc:christmas_carol (1,2)	Left:  3406634	Right:   137250	Length:       32	Text: and set him in the midst of them
Doc:oliver_twist.tx (1,8)	Left:  3878715	Right:   796294	Length:       32	Text: Your blood be upon your own head
Doc:oliver_twist.tx (1,8)	Left:  1506502	Right:   376123	Length:       31	Text: that was brought into the house
Doc:oliver_twist.tx (1,8)	Left:  1507334	Right:   376123	Length:       31	Text: that was brought into the house
Doc:oliver_twist.tx (1,8)	Left:  1821376	Right:   376123	Length:       31	Text: that was brought into the house
Doc:oliver_twist.tx (1,8)	Left:  1822484	Right:   376123	Length:       31	Text: that was brought into the house
Doc:david_copperfie (1,3)	Left:  3445580	Right:  1002902	Length:       43	Text: have done it unto one of the least of these
Doc:david_copperfie (1,3)	Left:  3353872	Right:  1606967	Length:       38	Text: thieves do not break through nor steal
Doc:david_copperfie (1,3)	Left:    68504	Right:  1057125	Length:       33	Text: up early in the morning, and took
Doc:david_copperfie (1,3)	Left:   105108	Right:  1057125	Length:       33	Text: up early in the morning, and took
Doc:david_copperfie (1,3)	Left:  1698448	Right:   889032	Length:       31	Text: t was in the front of the house
Doc:david_copperfie (1,3)	Left:  1975302	Right:  1630106	Length:       31	Text: the wicked cease from troubling
Doc:david_copperfie (1,3)	Left:  3446288	Right:  1002917	Length:       31	Text: to one of the least of these, y
Doc:david_copperfie (1,3)	Left:  1460768	Right:  1057125	Length:       30	Text: up early in the morning, and t
Doc:david_copperfie (1,3)	Left:  2598653	Right:   879087	Length:       30	Text: s, and laid the foundations of
Doc:david_copperfie (1,3)	Left:  3208034	Right:   673473	Length:       30	Text: ers into the hand of the child
Doc:david_copperfie (1,3)	Left:  3354972	Right:   653547	Length:       30	Text: toil not, neither do they spin

Successes: 0	 Failures:0
```

Installation from source
------------------------

The development version of Go (tip) is required to build due to various features that aren't present in 1.0. Also required are [git](http://git-scm.com/), [bzr](http://bazaar.canonical.com/en/) and [mercurial](http://mercurial.selenic.com/) to enable the dependencies to be checked out.


```bash
hg clone https://code.google.com/p/go/
cd go/src
./make.bash

// Put these in your .bash_profile
// export GOROOT=/path/to/go
// export PATH=$PATH:/path/to/go/bin

cd <working directory>

git clone https://github.com/donovanhide/superfastmatch-go.git
cd superfastmatch-go
export GOPATH=`pwd`
make dependencies
make run
```

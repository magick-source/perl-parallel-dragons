# Parallel-Dragons

Do you need to run some code every 2 seconds, but no more than 5 times?
Now you can - that's the basic usage of Parallel::Dragons.

Do you have a queue and need to run a block of code for each element
of the queue? That's easy - Parallel::Dragons can help you do that.

Parallel::Dragons can be used in a queue driven mode or without a queue.

In the first case it forks (some) children to run elements of the queue
and keeps fetching items from the queue and processing them.

In the second case, each children will run main() once and return - and
later a new children will be started.

## INSTALLATION

To install this module type the following:

   perl Makefile.PL
   make
   make test
   make install

## DEPENDENCIES

this module requires several other modules, but all are usually part
of perl core:

  Carp
  Socket
  IO::Select
  IO::Socket::UNIX
  Time::HiRes
  Data::Dumper

This module requires these other modules:

  Time::HiRes - it sleeps for subsecond intervals when idle

## SUPPORT AND BUGS

the main issues tracking of this project is in

  http://magick-source.net/MagickPerl/Parallel-Dragons/

## ACKNOLEDGEMENT

Most of this code was written while working on booking.com projects
during booking.com time - the code was later generalized and move
to a namespace that makes (a bit) more sense.

## COPYRIGHT AND LICENCE

This is licensed with GPL 2.0+ or perl's artistic licence
the files with both licences are part of this package

Copyright (C) 2016 by theMage

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.22.1 or,
at your option, any later version of Perl 5 you may have available.

Alternativally, you can also redistribute it and/or modify it
under the terms of the GPL 2.0 licence (or any future version of it).


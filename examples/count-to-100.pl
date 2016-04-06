#!/usr/bin/perl

use strict;
use warnings;

use parent qw'Parallel::Dragons';

my $parent = $$;

main::->new()->run();

sub max_childs { 5 }

sub wait_before_restart { 10 }

sub main {
  my $i = 0;
  while ($i++ < 100) {
    print "counted to $i\n";
    sleep 1 if $parent != $$;
  }
}

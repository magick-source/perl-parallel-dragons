use strict;
use warnings;

use Test::More;

my @out = qx{$^X -Ilib examples/count-to-100.pl startfg};
is(scalar @out, 100, 'got 100 lines');

my @cnt100 = grep { $_ eq "counted to 100\n" } @out;
is(scalar @cnt100, 1, 'got up to 100');

done_testing();

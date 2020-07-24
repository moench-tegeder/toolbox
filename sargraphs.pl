#!/usr/bin/env perl
#
# sargraphs.pl - generate graphs from atop data using gnuplot
#
# (c) 2020 Christoph Moench-Tegeder <cmt@burggraben.net>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#


use strict;
use warnings;

use File::Temp qw(tempdir);
use Getopt::Std;


sub main {
  my $optstr = 'd:e:f:hp:s:t:vAH:MW:';
  my $opts = {};
  my $args = {};
  my ($obj, $r);

  $Getopt::Std::STANDARD_HELP_VERSION = 1;

  $r = getopts($optstr, $opts);
  if(!$r || exists($opts->{'h'})) {
    version();
    usage();
    return ($r ? 0 : 1);
  }
  if(exists($opts->{'v'})) {
    version();
    return 0;
  }

  if(exists($opts->{'f'})) {
    $args->{'infile'} = $opts->{'f'};
  }

  if(exists($opts->{'t'})) {
    if($opts->{'t'} =~ /^(?:cpu|iobw|iops)$/i) {
      $args->{'type'} = lc($opts->{'t'});
    } else {
      print(STDERR 'wrong type given' . "\n");
      return 1;
    }
  }
  if(exists($opts->{'d'})) {
    $args->{'devices'} = {
      map {
        $_ =~ /^([a-zA-Z0-9\-]+):(.*)$/ ? ($1 => $2) : ($_ => $_)
      } (split(',', $opts->{'d'}))
    };
  }

  if(exists($opts->{'p'})) {
    $args->{'outfile'} = $opts->{'p'};
  }

  if(exists($opts->{'s'})) {
    $args->{'start'} = $opts->{'s'};
  }
  if(exists($opts->{'e'})) {
    $args->{'end'} = $opts->{'e'};
  }

  if(exists($opts->{'H'})) {
    $args->{'height'} = $opts->{'H'};
  }
  if(exists($opts->{'W'})) {
    $args->{'width'} = $opts->{'W'};
  }

  if(exists($opts->{'A'})) {
    $args->{'avg'} = 1;
    $args->{'max'} = 0;
  }
  if(exists($opts->{'M'})) {
    $args->{'avg'} = 0;
    $args->{'max'} = 1;
  }

  $obj = Internal::SARGrapher->new($args);
  if(!defined($obj)) {
    print(STDERR 'initialisation failed!' . "\n");
    return 1;
  }

  $r = $obj->assemble_data();
  if($r != 0) {
    print(STDERR 'error extracting data' . "\n");
    return 1;
  }

  $r = $obj->do_plot();

  return $r;
}


sub me {
  print('sargraphs.pl - gnuplot graphs from sar data' . "\n");

  return;
}


sub usage {
  print(' -d dev     ' . "\t" . 'IO: comma-seperated list of devices' . "\n");
  print('            ' . "\t" . 'device aliases: device:alias[,...]' . "\n");
  print(' -s 12:34:56' . "\t" . 'start of graph (sar timestamp)' . "\n");
  print(' -e 12:34:56' . "\t" . 'end of graph (sar timestamp)' . "\n");
  print(' -f file    ' . "\t" . 'input file (sar binary archive)' . "\n");
  print(' -h         ' . "\t" . 'help (this output)' . "\n");
  print(' -p out.png ' . "\t" . 'plot (output) file' . "\n");
  print(' -t type    ' . "\t" . 'plot type: CPU, IOBW, IOPS' . "\n");
  print(' -v         ' . "\t" . 'version info' . "\n");
  print(' -A         ' . "\t" . 'plot averages only' . "\n");
  print(' -H height  ' . "\t" . 'graph height' . "\n");
  print(' -M         ' . "\t" . 'plot maximums only' . "\n");
  print(' -W width   ' . "\t" . 'graph width' . "\n");
  print(' Default plot type is "CPU".' . "\n");
  print(' Alternative plot types: IOBW (IO bandwith)' . "\n");
  print('                         IOPS (IO Operations per second)' . "\n");
  print("\n");

  return;
}


sub version {
  me();
  print('Version 1.0' . "\n\n");
  return;
}


sub HELP_MESSAGE {
  usage();

  return;
}


sub VERSION_MESSAGE {
  version();

  return;
}

exit(main());


package Internal::SARGrapher;

use strict;
use warnings;

use File::Copy;
use File::Spec;
use File::Temp qw(tempdir);
use IPC::Cmd qw(can_run);
use POSIX;
use Storable qw(dclone);


sub new {
  my ($class, $args) = (shift, shift);
  my $dflts = {
    'infile' => undef,
    'outfile' => 'plot.png',
    'width' => 1200,
    'height' => 900,
    'type' => 'cpu',
    'gnuplot' => undef,
    'sar' => undef,
    'max' => 1,
    'avg' => 1,
    'start' => '00:00:00',
    'end' => '23:59:59',
    'devices' => {},
    'workdir' => undef
  };
  my ($self, $module);

  if($args->{'type'} eq 'cpu') {
    $module = 'Internal::SARGrapher::CPU';
  } elsif($args->{'type'} eq 'iobw') {
    $module = 'Internal::SARGrapher::IOBW';
  } elsif($args->{'type'} eq 'iops') {
    $module = 'Internal::SARGrapher::IOPS';
  } else {
    return undef;
  }

  if(!(exists($args->{'infile'}) && -r $args->{'infile'})) {
    print(STDERR 'No input file given' . "\n");
    return undef;
  }

  $dflts->{'gnuplot'} = can_run('gnuplot');
  $dflts->{'sar'} = can_run('sar');
  if(!(defined($dflts->{'gnuplot'}) && -x $dflts->{'gnuplot'})) {
    print(STDERR 'Cannot find gnuplot in PATH' . "\n");
    return undef;
  }
  if(!(defined($dflts->{'sar'}) && -x $dflts->{'sar'})) {
    print(STDERR 'Cannot find sar in PATH' . "\n");
    return undef;
  }

  $self = $module->new(dclone({%{$dflts}, %{$args}}));
  if(!defined($self)) {
    return undef;
  }

  $self->{'workdir'} = tempdir('CLEANUP' => 1);
  if(!$self->{'workdir'}) {
    print(STDERR 'Cannot create temp dir: ' . $! . "\n");
    return undef;
  }
  $self->{'tmpplot'} = 'tmpplot.png';

  return $self;
}


sub do_plot {
  my $self = shift;
  my ($data, $intro, $plot, $r);

  $intro = $self->gen_intro();
  $plot = $self->_do_plot();
  $data = $self->_mangle_data();
  $r = $self->make_graph($intro, $plot, $data);

  return $r;
}


sub gen_intro {
  my ($self, $type) = (shift, shift);
  my $intro = [];
  my ($file, $terminal);

  $file = File::Spec->catfile($self->{'workdir'}, $self->{'tmpplot'});
  $terminal = 'set terminal pngcairo font "LucidaSansDemiBold,10" ';
  $terminal.= 'size ' . $self->{'width'} . ', ' . $self->{'height'};
  $intro = [
    '#!/usr/bin/env gnuplot',
    '',
    $terminal,
    'set termoption dash',
    'set xdata time',
    'set timefmt "%H:%M:%S"',
    'set format x "%H:%M:%S"',
    'set tics out',
    @{$self->{'_ylabels'}},
    'set xtics nomirror',
    'set nox2tics',
    'set ytics nomirror',
    'set y2tics nomirror',
    'set border 11',
    'set linetype 1 lw 1 pt 1 ps 0.3',
    'set linetype 2 lw 1 pt 1 ps 0.3',
    'set linetype 3 lw 1 pt 1 ps 0.3',
    'set linetype 4 lw 1 pt 1 ps 0.3',
    'set linetype 5 lw 1 pt 1 ps 0.3',
    'set style line  1 lt 1 lc rgb "dark-violet"',
    'set style line  2 lt 1 lc rgb "sea-green"',
    'set style line  3 lt 1 lc rgb "cyan"',
    'set style line  4 lt 1 lc rgb "dark-red"',
    'set style line  5 lt 1 lc rgb "blue"',
    'set style line  6 lt 1 lc rgb "dark-orange"',
    'set style line  7 lt 1 lc rgb "black"',
    'set style line  8 lt 1 lc rgb "goldenrod"',
    'set style line  9 lt 1 lc rgb "light-red"',
    'set style line 10 lt 1 lc rgb "dark-grey"',
    'set style line 11 lt 2 lc rgb "dark-grey"',
    'set style line 12 lt 3 lc rgb "dark-grey"',
    'set style line 13 lt 4 lc rgb "dark-grey"',
    'set style line 14 lt 5 lc rgb "dark-grey"',
    'set style line 15 lt 1 lc rgb "sandybrown"',
    'set style line 16 lt 2 lc rgb "sandybrown"',
    'set style line 17 lt 3 lc rgb "sandybrown"',
    'set style line 18 lt 4 lc rgb "sandybrown"',
    'set style line 19 lt 5 lc rgb "sandybrown"',
    'set style line 20 lt 1 lc rgb "purple"',
    'set style line 21 lt 2 lc rgb "purple"',
    'set style line 22 lt 3 lc rgb "purple"',
    'set style line 23 lt 4 lc rgb "purple"',
    'set style line 24 lt 5 lc rgb "purple"',
    'set style line 25 lt 1 lc rgb "violet"',
    'set style line 26 lt 2 lc rgb "violet"',
    'set style line 27 lt 3 lc rgb "violet"',
    'set style line 28 lt 4 lc rgb "violet"',
    'set style line 29 lt 5 lc rgb "violet"',
    'set output "' . $file . '"'
  ];

  return $intro;
}


sub make_graph {
  my ($self, $intro, $toplot, $data) = (shift, shift, shift, shift);
  my $datafile = $self->{'workdir'} . '/sar.data';
  my $gpfile = $self->{'workdir'} . '/sar.gp';
  my ($fh, $plot, $res, $src, $r);

  $r = open($fh, '>', $gpfile);
  if(!$r) {
    print(STDERR 'cannot write gnuplot file: ' . $! . "\n");
    return -1;
  }
  $plot = [ map { ' "' . $datafile . '" using 1:' . $_ } (@{$toplot}) ];
  print($fh join("\n", @{$intro}) . "\n\n" . 'plot\\' . "\n");
  print($fh join(", \\\n", @{$plot}) . "\n");
  close($fh);

  $r = open($fh, '>', $datafile);
  if(!$r) {
    print(STDERR 'cannot write to data file: ' . $! . "\n");
    return -1;
  }
  print($fh join("\n", @{$data}) . "\n");
  close($fh);

  $res = 0;
  $r = system($self->{'gnuplot'}, $gpfile);
  if($r == 0) {
    $src = File::Spec->catfile($self->{'workdir'}, $self->{'tmpplot'});
    $r = copy($src, $self->{'outfile'});
    if($r == 0) {
      print(STDERR 'failed to copy output: ' . $! . "\n");
      $res = -1;
    }
  } else {
    print(STDERR 'failed to run gnuplot: ' . $! . "\n");
    $res = -1;
  }

  return $res;
}


sub assemble_data {
  my $self = shift;
  my $h = {};
  my ($ch, $cpu, $dev, $dh, $lh, $load, $rd, $t, $ts, $r);

  $r = 0;

  $cpu = $self->get_cpu_data();
  if(!defined($cpu) || scalar(@{$cpu} < 2)) {
    print(STDERR 'malformed data for CPU' . "\n");
    $r = -1;
  }
  $ch = shift(@{$cpu});
  $ch->[0] = 'ts';

  $load = $self->get_load_data();
  if(!defined($load) || scalar(@{$load} < 2)) {
    print(STDERR 'malformed data for load' . "\n");
    $r = -1;
  }
  $lh = shift(@{$load});
  $lh->[0] = 'ts';

  $dev = $self->get_dev_data();
  if(!defined($dev) || scalar(@{$dev} < 2)) {
    print(STDERR 'malformed data for devices' . "\n");
    $r = -1;
  }
  $dh = shift(@{$dev});
  $dh->[0] = 'ts';

  foreach $rd (@{$cpu}) {
    $h->{$rd->[0]} = { map { $ch->[$_] => $rd->[$_] } (2..$#{$ch}) };
  }

  foreach $rd (@{$load}) {
    $h->{$rd->[0]} = {
      %{$h->{$rd->[0]}}, map { $lh->[$_] => $rd->[$_] } (1..$#{$lh})
    };
  }

  # dev is different
  foreach $rd (@{$dev}) {
    $h->{$rd->[0]} = {
      %{$h->{$rd->[0]}},
      map {
        $rd->[1] ne 'DEV' ? ($dh->[$_] . '_' . $rd->[1] => $rd->[$_]) : ()
      } (2..$#{$dh})
    };
  }

  $self->{'_data'} = [
    map { {%{$h->{$_}}, 'ts' => $_} } (sort { $a cmp $b } (keys(%{$h})))
  ];

  return (scalar(@{$self->{'_data'}}) > 2 ? $r : -1);
}


sub _do_plot {
  my $self = shift;
  my $plot;

  if($self->{'avg'}) {
    $plot = $self->_plot_data('avg', 'lines');
  }
  if($self->{'max'}) {
    $plot = $self->_plot_data('max', 'points');
  }

  return $plot;
}


sub get_cpu_data {
  my $self = shift;
  my $args = ['-u', 'ALL'];

  return $self->get_data_from_sar($args);
}


sub get_load_data {
  my $self = shift;
  my $args = ['-q'];

  return $self->get_data_from_sar($args);
}


sub get_dev_data {
  my $self = shift;
  my $args = ['-d'];
  
  if(scalar(keys(%{$self->{'devices'}}))) {
    push(@{$args}, '--dev=' . join(',', (keys(%{$self->{'devices'}}))));
  }

  return $self->get_data_from_sar($args);
}


sub get_data_from_sar {
  my ($self, $args) = (shift, shift);
  my $rows = [];
  my ($cmd, $fh, $l, $r);

  $cmd = [
    $self->{'sar'}, @{$args}, '-f', $self->{'infile'},
    '-s', $self->{'start'}, '-e', $self->{'end'}
  ];
  $r = open($fh, '-|', @{$cmd});
  if(!$r) {
    print(STDERR 'error running sar: ' . $! . "\n");
    return undef;
  }

  while($l = <$fh>) {
    chomp($l);
    if($l eq '' || $l !~ /^\d/) {
      next;
    }
    push(@{$rows}, [split(/\s+/, $l)]);
  }

  close($fh);

  return $rows;
}


sub _mangle_data {
  my $self = shift;
  my $list = [];
  my ($d, $dpp, $i, $keys, $res, $row, $tmp, $v);

  $keys = $self->_columns();

  foreach $row (@{$self->{'_data'}}) {
    push(@{$list}, [map { $row->{$_} } (@{$keys})]);
  }

  $dpp = ceil(3 * scalar(@{$self->{'_data'}}) / $self->{'width'});
  while(scalar(@{$list}) > 0) {
    $row = [];
    $tmp = [splice(@{$list}, 0, $dpp)];
    for($i = 0; $i <= $#{$tmp->[0]}; $i++) {
      $d = [map { $i > 0 ? 0 + $_->[$i] : $_->[$i] } (@{$tmp})];
      push(@{$row}, $i == 0 ? $d->[0] : avg($d), $i > 0 ? max($d) : ());
    }
    push(@{$res}, join(' ', @{$row}));
  }

  return $res;
}


sub avg {
  my $in = shift;
  my $avg = 0;
  my $t;

  foreach $t (@{$in}) {
    $avg += $t;
  }
  $avg /= scalar(@{$in});

  return $avg;
}


sub max {
  my $in = shift;
  my ($max, $t);

  foreach $t (@{$in}) {
    $max = (defined($max) && $max > $t) ? $max : $t;
  }

  return $max;
}


sub _intro_ylabels {
  my $self = shift;

  return dclone($self->{'_ylabels'});
}


sub _plot_cols {
  my $self = shift;

  return dclone($self->{'_plot_cols'});
}

1;


package Internal::SARGrapher::CPU;

use strict;
use warnings;

use base qw(Internal::SARGrapher);


sub new {
  my ($class, $args) = (shift, shift);
  my $self;

  $self = { %{$args} };
  $self->{'_ylabels'} = ['set ylabel "%"', 'set y2label "#"'];
  $self->{'_plotfmt'} = '%d axes x1y1 title "CPU %s" with %s ls %d';
  $self->{'_plot_cols'} = [
    {'key' => '%usr', 'descr' => 'User%'},
    {'key' => '%sys', 'descr' => 'System%'},
    {'key' => '%iowait', 'descr' => 'IOWait%'}
  ];

  return bless($self, $class);
}


sub _plot_data {
  my ($self, $func, $style) = (shift, shift, shift);
  my $cols = $self->_plot_cols();
  my $dc = $self->_data_keys();
  my $fmt = $self->{'_plotfmt'};
  my $plot = [];
  my ($i, $idx, $str);

  for($i = 0; $i <= $#{$cols}; $i++) {
    $idx = 1 + $dc->{uc($func) . '_' . $cols->[$i]->{'key'}};
    $str = sprintf($fmt, $idx, $cols->[$i]->{'descr'}, $style, $i + 1);
    push(@{$plot}, $str);
  }

  return $plot;
}


sub _columns {
  my $self = shift;

  return ['ts', (map { $_->{'key'} } (@{$self->_plot_cols()}))];
}


sub _data_keys {
  my $self = shift;
  my ($dk, $k, $v);

  $k = $self->_columns();
  $dk = {
    map {
      $v = $k->[$_];
      $_ == 0 ? ($v => $_) : ('AVG_' . $v => 2 * $_ - 1, 'MAX_' . $v => 2 * $_)
    } (0..$#{$k})
  };

  return $dk;
}

1;


package Internal::SARGrapher::IO;

use strict;
use warnings;

use base qw(Internal::SARGrapher);


sub _plot_data {
  my ($self, $func, $style) = (shift, shift, shift);
  my $cols = $self->_plot_cols();
  my $dc = $self->_data_keys();
  my $keys = $self->_colidx();
  my $fmt = $self->{'_plotfmt'};
  my $plot = [];
  my ($c, $col, $d, $dcols, $i, $idx, $str, $t);

  $i = 0;
  foreach $c (@{$cols}) {
    $dcols = [
      grep {
        $_->{'func'} eq uc($func) && $_->{'type'} eq $c->{'key'}
      } (@{$dc})
    ];
    foreach $t (@{$dcols}) {
      if(!exists($keys->{uc($func) . '_' . $c->{'key'} . '_' . $t->{'dev'}})) {
        print(uc($func) . '_' . $t->{'key'} . '_' . $t->{'dev'} . "\n");
        exit(1);
      }
      $idx = 1 + $keys->{uc($func) . '_' . $c->{'key'} . '_' . $t->{'dev'}};
      if(exists($self->{'devices'}->{$t->{'dev'}})) {
        $d = $self->{'devices'}->{$t->{'dev'}};
      } else {
        $d = $t->{'dev'};
      }
      $col = 1 + $i % 10;
      $str = sprintf($fmt, $idx, $c->{'axis'}, $c->{'descr'}, $d, $style, $col);
      push(@{$plot}, $str);
      $i++;
    }
  }

  return $plot;
}


sub _data_keys {
  my $self = shift;
  my $re = qr/^(AVG|MAX)_([^_]+)_(.*)$/;
  my ($dk, $k);

  $k = $self->_colidx();
  $dk = [
    sort { $a->{'dev'} cmp $b->{'dev'} } (
      map {
        ($_ =~ /$re/) ? {'func' => $1, 'type' => $2, 'dev' => $3} : ()
      } (keys(%{$k}))
    )
  ];

  return $dk;
}


sub _columns {
  my $self = shift;
  my $cols = $self->_plot_cols();
  my $keys = ['ts'];
  my ($k, $re, $t);

  foreach $t (@{$cols}) {
    $k = $t->{'key'};
    push(@{$keys}, sort(grep {/^$k/} (keys(%{$self->{'_data'}->[0]}))));
  }

  return $keys;
}


sub _colidx {
  my $self = shift;
  my $cols = $self->_columns();
  my ($idx, $v);

  $idx = {
    map {
      $v = $cols->[$_];
      $_ == 0 ? ($v => $_) : ('AVG_' . $v => 2 * $_ -1, 'MAX_' . $v => 2 * $_)
    } (0..$#{$cols})
  };

  return $idx;
}

1;


package Internal::SARGrapher::IOBW;

use strict;
use warnings;

use base qw(Internal::SARGrapher::IO);


sub new {
  my ($class, $args) = (shift, shift);
  my $self;

  $self = { %{$args} };
  $self->{'_ylabels'} = ['set ylabel "kB/s"', 'set y2label "kB"'];
  $self->{'_plotfmt'} = '%d axes x1y%d title "%s %s" with %s ls %d';
  $self->{'_plot_cols'} = [
    {'key' => 'rkB/s', 'descr' => 'kB/s read', 'axis' => 1},
    {'key' => 'wkB/s', 'descr' => 'kB/s write', 'axis' => 1},
    {'key' => 'areq-sz', 'descr' => 'avg req size', 'axis' => 2}
  ];

  return bless($self, $class);
}

1;


package Internal::SARGrapher::IOPS;

use strict;
use warnings;

use base qw(Internal::SARGrapher::IO);


sub new {
  my ($class, $args) = (shift, shift);
  my $self;

  $self = { %{$args} };
  $self->{'_ylabels'} = ['set ylabel "IOPS"', 'set y2label "latency"'];
  $self->{'_plotfmt'} = '%d axes x1y%d title "%s %s" with %s ls %d';
  $self->{'_plot_cols'} = [
    {'key' => 'tps', 'descr' => 'IOPS', 'axis' => 1},
    {'key' => 'await', 'descr' => 'IO latency', 'axis' => 2}
  ];

  return bless($self, $class);
}

1;

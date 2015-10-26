#!/usr/bin/perl
#
# atopplot.pl - generate graphs from atop data using gnuplot
#
# (c) 2013-2015 Christoph Moench-Tegeder <cmt@burggraben.net>
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
# $Id: atopplot.pl 1519 2015-10-26 21:17:23Z cmt $
#

use strict;
use warnings;

use Getopt::Std;
use IPC::Cmd qw(can_run);
use POSIX;

my $dskdevs = [];

sub main {
  my $optstr = 'vghMAb:e:o:p:r:R:D:F:W:H:';
  my $outfile = 'atop.gp';
  my $datafile = 'atop.data';
  my $pic = 'atoplog.png';
  my $fmap = {
    'mem' => 1,
    'cpu' => 1,
    'dsk' => 1,
    'io' => 0
  };
  my $opts ={};
  my $args = {};
  my $pargs = {};
  my $feats = [];
  my $run = 0;
  my ($f, $file, $pd, $start, $stop, $r);

  $args = {
    'imgfile' => $pic,
    'gpfile' =>  $outfile,
    'datafile' => $datafile,
    'width' => 1200,
    'height' => 900,
    'max' => 1,
    'avg' => 1,
    'iostat' => []
  };

  $r = getopts($optstr, $opts);
  if(!$r || exists($opts->{'h'})) {
    usage();
    return ($r ? 0 : 1);
  }
  if(exists($opts->{'v'})) {
    version();
    return 0;
  }

  if(exists($opts->{'g'})) {
    $f = can_run('gnuplot');
    if(!(defined($f) && -x $f)) {
      print(STDERR 'gnuplot binary not found' . "\n");
      return 1;
    }
    $args->{'gnuplot'} = $f;
    $run = 1;
  }

  if(exists($opts->{'r'}) && exists($opts->{'R'})) {
    print(STDERR 'need either raw file (r) or ASCII log (R)' . "\n");
    return 1;
  }
  if(!exists($opts->{'r'}) && !exists($opts->{'R'})) {
    print(STDERR 'need one of raw file (r) or ASCII log (R)' . "\n");
    return 1;
  }

  if(exists($opts->{'r'}) && $opts->{'r'}) {
    $pargs->{'rawfile'} = $opts->{'r'};
    if(! -r $pargs->{'rawfile'}) {
      print(STDERR 'cannot read ' . $pargs->{'rawfile'} . "\n");
      return 1;
    }
  }

  if(exists($opts->{'R'}) && $opts->{'R'}) {
    $pargs->{'pfile'} = $opts->{'R'};
    if(!-r $pargs->{'pfile'}) {
      print(STDERR 'cannot read ' . $pargs->{'pfile'} . "\n");
      return 1;
    }
  }

  if(exists($opts->{'D'}) && $opts->{'D'}) {
    $args->{'datafile'} = $opts->{'D'};
  }

  if(exists($opts->{'o'}) && $opts->{'o'}) {
    $args->{'gpfile'} = $opts->{'o'};
  }
  if(exists($opts->{'p'}) && $opts->{'p'}) {
    $args->{'imgfile'} = $opts->{'p'};
  }

  if(exists($opts->{'b'}) && $opts->{'b'}) {
    $pargs->{'start'} = $opts->{'b'};
  }
  if(exists($opts->{'e'}) && $opts->{'e'}) {
    $pargs->{'stop'} = $opts->{'e'};
  }
  if(exists($opts->{'W'}) && $opts->{'W'}) {
    $args->{'width'} = $opts->{'W'};
  }
  if(exists($opts->{'H'}) && $opts->{'H'}) {
    $args->{'height'} = $opts->{'H'};
  }

  if(exists($opts->{'A'}) || exists($opts->{'M'})) {
    if(!exists($opts->{'A'})) {
      $args->{'avg'} = 0;
    }
    if(!exists($opts->{'M'})) {
      $args->{'max'} = 0;
    }
  }

  if(exists($opts->{'F'}) && $opts->{'F'}) {
    $feats = [ split(/,/, $opts->{'F'}) ];
    foreach $f (keys(%{$fmap})) {
      $fmap->{$f} = 0;
    }
    foreach $f (@{$feats}) {
      if($f =~ /^io:([a-z]+)$/) {
        $f = 'io';
        push(@{$args->{'iostat'}}, $1);
      }
      if(!exists($fmap->{$f})) {
        print(STDERR 'unknown feature ' . $f . "\n");
        next;
      }
      $fmap->{$f} = 1;
    }
  }
  $args->{'features'} = $fmap;
  if($args->{'features'}->{'io'} && scalar(@{$args->{'iostat'}}) == 0) {
    $args->{'iostat'} = ['ALL'];
  }

  $pd = process($pargs);
  if(!defined($pd)) {
    printf(STDERR 'error extracting data' . "\n");
    return 1;
  } elsif(scalar(@{$pd}) == 0) {
    print(STDERR 'no data extracted' . "\n");
    return 1;
  }
  $args->{'data'} = $pd;

  $r = make_plot_txt($args);
  if($r == 0 && $run) {
    $r = do_plot($args);
  }

  return ($r ? 1 : 0);
}


sub process {
  my $args = shift;
  my $cmdline = [
    '/usr/bin/atop',
    '-r', $args->{'rawfile'},
    '-P', 'CPU,MEM,DSK'
  ];
  my $record = {};
  my $data = [];
  my $plot = [];
  my $ascread = 0;
  my ($line, $mu, $offset,  $oldint, $out, $tt, $tvd, $r);

  if(exists($args->{'rawfile'})) {
    if(defined($args->{'start'})) {
      push(@{$cmdline}, '-b', $args->{'start'});
    }
    if(defined($args->{'stop'})) {
      push(@{$cmdline}, '-e', $args->{'stop'});
    }
    $r = open(ATOP, '-|', @{$cmdline});
    if(!$r) {
      print(STDERR 'error starting atop: ' . $! . "\n");
      return undef;
    }
  } elsif(exists($args->{'pfile'})) {
    $ascread = 1;
    $r = open(ATOP, '<', $args->{'pfile'});
    if(!$r) {
      print(STDERR 'error reading input: ' . $! . "\n");
      return undef;
    }
  }

  $oldint = 0;
  while($line = <ATOP>) {
    chomp($line);
    if($line eq 'RESET') {
      next;
    }
    if($line eq 'SEP') {
      if(scalar(keys(%{$record})) && $record->{'ts'} > 0) {
        if($oldint > $record->{'interval'}) {
          pop(@{$plot});
        }
        $record->{'dsk_pct'} /= scalar(keys(%{$record->{'dskunits'}}));
        $out = fmt_line($record);
        push(@{$plot}, $out);
        $oldint = $record->{'interval'};
      }
      $record = {
        'ts' => -1,
        'interval' => 0,
        'cpu_u' => 0, 'cpu_s' => 0, 'cpu_w' => 0,
        'mem_pct_u' => 0,
        'dsk_pct' => 0,
        'dsk_r_req' => 0, 'dsk_r_blk' => 0, 'dsk_w_req' => 0, 'dsk_w_blk' => 0,
        'dskunits' => {}
      };
      next;
    }
    $data = [ split(/\s+/, $line) ];
    if($ascread) {
      if(cmptime($args->{'start'}, $data->[3], $data->[4]) < 0) {
        next;
      }
      if(cmptime($args->{'stop'}, $data->[3], $data->[4]) > 0) {
        last;
      }
    }
    $tvd = $data->[5];
    if(!defined($offset)) {
      $offset = chk_timeoffset($data->[2], $data->[3], $data->[4]);
    }
    $record->{'ts'} = $data->[2] + $offset;
    $record->{'interval'} = $tvd;
    if($data->[0] eq 'CPU') {
      # totalticks = interval * ticks * cpus
      $tt = $tvd * $data->[6] * $data->[7];
      $record->{'cpu_s'} = sprintf('%.2f', 100.0 * ($data->[8] / $tt));
      $record->{'cpu_u'} = sprintf('%.2f', 100.0 * ($data->[9] / $tt));
      $record->{'cpu_w'} = sprintf('%.2f', 100.0 * ($data->[12] / $tt));
    } elsif($data->[0] eq 'MEM') {
      $mu = $data->[7] - ($data->[8] + $data->[9] + $data->[10]);
      $record->{'mem_pct_u'} = sprintf('%.2f', 100 * ($mu / $data->[7]));
    } elsif($data->[0] eq 'DSK') {
      # (busy(ms) / interval(s)) * 100%
      $record->{'dsk_pct'} += sprintf('%.2f', $data->[7] / ($tvd * 10.0));
      $record->{'dsk_r_req'} += $data->[8] / $tvd;
      $record->{'dsk_r_blk'} += $data->[9] / $tvd;
      $record->{'dsk_w_req'} += $data->[10] / $tvd;
      $record->{'dsk_w_blk'} += $data->[11] / $tvd;
      $record->{'dskunits'}->{$data->[6]} = {
        'pct' => sprintf('%.2f', $data->[7] / ($tvd * 10.0)),
        'r_req' => $data->[8] / $tvd,
        'r_blk' => $data->[9] / $tvd,
        'w_req' => $data->[10] / $tvd,
        'w_blk' => $data->[11] / $tvd
      };
    }
  }

  close(ATOP);

  return $plot;
}


sub fmt_line {
  my $rec = shift;
  my $fields = [qw(
    ts
    cpu_u cpu_s cpu_w
    mem_pct_u
    dsk_pct
    dsk_r_req dsk_r_blk dsk_w_req dsk_w_blk
  )];
  my $dskfields = [qw(
    pct r_req r_blk w_req w_blk
  )];
  my $d = [];
  my $io = [];
  my $disks = { map { $_ => 1 } (@{$dskdevs}) };
  my ($dsk, $l, $p);

  foreach $dsk (sort(keys(%{$rec->{'dskunits'}}))) {
    if(!exists($disks->{$dsk})) {
      push(@{$dskdevs}, $dsk);
    }
  }
  $d = [ map { exists($rec->{$_}) ? $rec->{$_} : 0 } (@{$fields}) ];
  foreach $dsk (@{$dskdevs}) {
    $p = $rec->{'dskunits'}->{$dsk};
    $io = [ map { exists($p->{$_}) ? $p->{$_} : 0 } @{$dskfields} ];
    push(@{$d}, @{$io});
  }
  $l = join(' ', @{$d});

  return $l;
}


sub mangle_data {
  my ($data, $px) = (shift, shift);
  my $procdata = [];
  my $row = [];
  my $tmp = [];
  my $d = [];
  my ($dpp, $i);

  $dpp = ceil(3 * scalar(@{$data}) / $px);
  while(scalar(@{$data}) > 0) {
    $row = [];
    $tmp = [ map { [ split(/\s+/, $_) ] } (splice(@{$data}, 0, $dpp)) ];
    for($i = 0; $i < scalar(@{$tmp->[0]}); $i++) {
      $d = [ map { $_->[$i] } (@{$tmp}) ];
      push(@{$row}, avg($d));
      if($i > 0) {
        push(@{$row}, max($d));
      }
    }
    push(@{$procdata}, join(' ', @{$row}));
  }

  return $procdata;
}


sub avg {
  my $data = shift;
  my ($avg, $n, $t);

  $avg = 0;
  $n = scalar(@{$data});
  foreach $t (@{$data}) {
    $avg += $t;
  }
  $avg /= $n;

  return $avg;
}


sub max {
  my $data = shift;
  my ($max, $t);

  foreach $t (@{$data}) {
    $max = (defined($max) && $max > $t) ? $max : $t;
  }

  return $max;
}


sub make_plot_txt {
  my $args = shift;
  my $data = [];
  my $intro = [];
  my $iodevs = [];
  my $plot = [];
  my $out = [];
  my ($col, $dsk, $fh, $str, $type, $r);

  if($args->{'gpfile'} ne '-') {
    $r = open($fh, '>', $args->{'gpfile'});
    if(!$r) {
      print(STDERR 'error opening ' . $args->{'gpfile'} . ': ' . $!);
      return -1;
    }
  } else {
    $fh = \*STDOUT;
  }

  if($args->{'features'}->{'cpu'}) {
    if($args->{'avg'}) {
      push(@{$plot}, '2 axes x1y1 title "CPU User %" with lines ls 1');
      push(@{$plot}, '4 axes x1y1 title "CPU System %" with lines ls 2');
      push(@{$plot}, '6 axes x1y1 title "CPU I/O wait %" with lines ls 3');
    }
    if($args->{'max'}) {
      push(@{$plot}, '3 axes x1y1 title "CPU User % max" with points ls 1');
      push(@{$plot}, '5 axes x1y1 title "CPU System % max" with points ls 2');
      push(@{$plot}, '7 axes x1y1 title "CPU I/O wait % max" with points ls 3');
    }
  }
  if($args->{'features'}->{'mem'}) {
    if($args->{'avg'}) {
      push(@{$plot}, '8 axes x1y1 title "Memory use %" with lines ls 4');
    }
    if($args->{'max'}) {
      push(@{$plot}, '9 axes x1y1 title "Memory use % max" with points ls 4');
    }
  }
  if($args->{'features'}->{'dsk'}) {
    if($args->{'avg'}) {
      push(@{$plot}, '10 axes x1y1 title "Disk %" with lines ls 5');
      push(@{$plot}, '12 axes x1y2 title "read req" with lines ls 6');
      push(@{$plot}, '14 axes x1y2 title "read blocks" with lines ls 7');
      push(@{$plot}, '16 axes x1y2 title "write req" with lines ls 8');
      push(@{$plot}, '18 axes x1y2 title "write blocks" with lines ls 9');
    }
    if($args->{'max'}) {
      push(@{$plot}, '11 axes x1y1 title "Disk % max" with points ls 5');
      push(@{$plot}, '13 axes x1y2 title "read req max" with points ls 6');
      push(@{$plot}, '15 axes x1y2 title "read blocks max" with points ls 7');
      push(@{$plot}, '17 axes x1y2 title "write req max" with points ls 8');
      push(@{$plot}, '19 axes x1y2 title "write blocks max" with points ls 9');
    }
  }
  if($args->{'features'}->{'io'}) {
    if($args->{'iostat'}->[0] eq 'ALL') {
      @{$args->{'iostat'}} = @{$dskdevs};
    }
    $iodevs = { map { $_ => 1 } @{$args->{'iostat'}} };
    $col = 19;
    foreach $dsk (@{$dskdevs}) {
      foreach $type (qw(pct r_req r_blk w_req w_blk)) {
        $col ++;
        if($args->{'avg'} && exists($iodevs->{$dsk})) {
          $str = gendskline($dsk, $col, $type, 'avg');
          push(@{$plot}, $str);
        }
        $col ++;
        if($args->{'max'} && exists($iodevs->{$dsk})) {
          $str = gendskline($dsk, $col, $type, 'max');
          push(@{$plot}, $str);
        }
      }
    }
  }

  $out = [
    map { '  "' . $args->{'datafile'} . '" using 1:' . $_ } (@{$plot})
  ];

  $intro = gen_intro($args);
  print($fh join("\n", @{$intro}) . "\n\n");
  print($fh 'plot \\' . "\n");
  print($fh join(", \\\n", @{$out}) . "\n");

  if($args->{'gpfile'} ne '-') {
    close($fh);
  }

  $data = mangle_data($args->{'data'}, $args->{'width'});
  $r = open(OUT, '>', $args->{'datafile'});
  if(!$r) {
    print(STDERR 'cannot open ' . $args->{'datafile'});
    if($args->{'gpfile'} ne '-') {
      unlink($args->{'gpfile'});
    }
    return -1;
  }
  print(OUT join("\n", @{$data}) . "\n");
  close(OUT);

  return 0;
}


sub gendskline {
  my ($dsk, $col, $name, $type) = (shift, shift, shift, shift);
  my $axes = 'axes x1y' . ($name eq 'pct' ? '1' : '2');
  my $title = 'title "' . $dsk . ' ';
  my $style = 'with ' . ($type eq 'max' ? 'points' : 'lines');
  my ($ls, $str);

  if($name eq 'pct') {
    $title.= '%';
  } elsif($name eq 'r_req') {
    $title.= 'read req';
  } elsif($name eq 'r_blk') {
    $title.= 'read blocks';
  } elsif($name eq 'w_req') {
    $title.= 'write req';
  } elsif($name eq 'w_blk') {
    $title.= 'write blocks';
  } else {
    return undef;
  }
  $title.= ($type eq 'max' ? ' max' : '') . '"';
  $ls = 'ls ' . (($col + ($type eq 'max' ? 1 : 0)) / 2);

  $str = sprintf('%d %s %s %s %s', $col, $axes, $title, $style, $ls);

  return $str;
}


sub gen_intro {
  my $args = shift;
  my $intro = [];
  my $terminal;

  $terminal = 'set terminal pngcairo font "LucidaSansDemiBold,10" ';
  $terminal.= 'size ' . $args->{'width'} . ',' . $args->{'height'};
  $intro = [
    '#!/usr/bin/gnuplot',
    '',
    $terminal,
    'set termoption dash',
    'set xdata time',
    'set timefmt "%s"',
    'set format x "%H:%M:%S"',
    'set tics out',
    'set ylabel "%"',
    'set y2label "#"',
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
    'set output "' . $args->{'imgfile'} . '"'
  ];

  return $intro;
}


sub cmptime {
  my ($ref, $date, $time) = (shift, shift, shift);
  my $dre = qr/(\d{4})[\-\/](\d{2})[\-\/](\d{2})/;
  my $tre = qr/(\d+):(\d+)(?::(\d+))?/;
  my $refd = [];
  my $reft = [];
  my $curd = [];
  my $curt = [];
  my ($i, $rd, $rt, $tmp, $r);

  if(!defined($ref)) {
    return 0;
  }

  if($ref =~ /^([0-9\/]+)\s+([0-9:]+)$/) {
    ($rd, $rt) = ($1, $2);
    $refd = [ split(/\//, $rd) ];
    $reft = [ split(/:/, $rt) ];
  } elsif ($ref =~ /^([0-9:]+)$/) {
    $rt = $1;
    $tmp = [ (localtime(time())) ];
    $refd = [ $tmp->[5] + 1900, $tmp->[4] + 1, $tmp->[3] ];
    $reft = [ split(/:/, $rt) ];
  } else {
    return undef;
  }

  $curd = [ split(/\//, $date) ];
  for($i = 0; $i < 3; $i++) {
    if($r = ($refd->[$i] <=> $curd->[$i])) {
      return $r;
    }
  }

  $curt = [ split(/\:/, $time) ];
  for($i = 0; $i < 3; $i++) {
    if($r = ($reft->[$i] <=> $curt->[$i])) {
      return $r;
    }
  }

  return 0;
}


sub chk_timeoffset {
  my ($ts, $refd, $reft) = (shift, shift, shift);
  my $d = [];
  my $t = [];
  my ($gref, $off, $ref, $tm);

  $d = [ map { $_ + 0 } (split(/\//, $refd)) ];
  $t = [ map { $_ + 0 } (split(/:/, $reft)) ];
  $d->[0] -= 1900;
  $d->[1] -= 1;
  $d->[2] -= 1;
  $tm = [ gmtime($ts) ];
  $ref = mktime($t->[2], $t->[1], $t->[0], $d->[2], $d->[1], $d->[0]);
  $gref = mktime($tm->[0], $tm->[1], $tm->[2], $tm->[3], $tm->[4], $tm->[5]);

  $off = $ref - $gref;

  return $off;
}


sub do_plot {
  my $args = shift;
  my $cmd = [
    $args->{'gnuplot'}, $args->{'gpfile'}
  ];
  my ($status, $r);

  $r = system(@{$cmd});
  if($r < 0) {
    print(STDERR 'system() reports: ' . $r . ': ' . $! . "\n");
    return -1;
  } elsif($r > 0) {
    $status = WEXITSTATUS($r);
    print(STDERR 'gnuplot returned ' . $status . "\n");
    return -1;
  }

  return 0;
}


sub me {
  print('atopplot.pl - reformat atop data for gnuplot' . "\n");

  return;
}


sub usage {
  me();
  print(' -r file  ' . "\t" . 'read atop raw file' . "\n");
  print(' -R file  ' . "\t" . 'read atop parseable file' . "\n");
  print(' -b time  ' . "\t" . 'start at time' . "\n");
  print(' -e time  ' . "\t" . 'end at time' . "\n");
  print(' -o file  ' . "\t" . 'output gnuplot script' . "\n");
  print(' -p file  ' . "\t" . 'name of resulting image' . "\n");
  print(' -D file  ' . "\t" . 'output gnuplot data' . "\n");
  print(' -F feat  ' . "\t" . 'feat: comma-seperated cpu,mem,dsk,io' . "\n");
  print(' -W width ' . "\t" . 'graph width' . "\n");
  print(' -H height' . "\t" . 'graph height' . "\n");
  print(' -M       ' . "\t" . 'plot max only' . "\n");
  print(' -A       ' . "\t" . 'plot avg only' . "\n");
  print(' -g       ' . "\t" . 'run gnuplot' . "\n");
  print(' -v       ' . "\t" . 'version info' . "\n\n");
  print('  Default graphs are cpu, mem, dsk. When graphing io stats,' . "\n");
  print('  select devices as io:<device>, e.g. io:sda. Default is to' . "\n");
  print('  graph all devices. Graphing more than 4 devices at a time' . "\n");
  print('  is not supported yet' . "\n");

  return;
}


sub version {
  me();
  print('Version $Revision: 1519 $' . "\n");
  print('Date $Date: 2015-10-26 22:17:23 +0100 (Mon, 26 Oct 2015) $' . "\n\n");

  return;
}

exit(main());

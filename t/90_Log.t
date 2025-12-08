
use FindBin qw($Bin);
use lib $Bin;
use t_Common qw/oops/; # strict, warnings, Carp
use t_TestCommon # Test2::V0 etc.
  qw/t_like t_ok my_capture my_capture_merged $silent $verbose $debug/;
use File::Which qw/which/;

package Inner;
our @ISA = ('Outer');
use Data::Dumper::Interp;
use Spreadsheet::Edit::Log
  qw/fmt_call log_call nearest_call abbrev_call_fn_ln_subname/,
  ':btw=l=$lno/fn=$fname/pk=$pkg/p=$package';

sub new { my $class=shift; bless {tag => $_[0]}, $class }
sub get { my $self=shift; $self->{tag} }
sub _pvt_noretval {
  my $self = shift;
  log_call {self => $self}, \@_;
}
sub _pvt_1retval {
  my $self = shift;
  log_call {self => $self}, \@_, 42;
}
sub _pvt_multiretval {
  my $self = shift;
  log_call {self => $self}, \@_, [42,99,{aaa=>100}];
}
sub func1 {
  log_call \@_, [\"z:",\"","ab",\"ccc",\"",99,{aaa=>100},\"there it was",99];
}

our %SpreadsheetEdit_Log_Options = (
  fmt_object => sub{
    my $obj = $_[1];
    if (eval{ $obj->can('get') }) {
      return addrvis($obj).'['.$obj->get().']';
    } else {
      return addrvis $obj;
    }
  },
);

sub doeval {
  my ($self, $str) = @_;
  my $result = eval $str;
  return defined($result) ? "normal:$result" : "exc:$@";
}
sub xxxtest {
  my $result = "";
  return defined($result) ? "normal:$result" : "exc:$@";
}

package Outer;
sub new { my $class = shift; bless {inner=>Inner->new(@_)}, $class }
sub get              { my $self=shift; $self->{inner}->get(@_) }
sub meth_noretval    { my $self=shift; $self->{inner}->_pvt_noretval(@_); }
sub meth_1retval     { my $self=shift; $self->{inner}->_pvt_1retval(@_); }
sub meth_multiretval { my $self=shift; $self->{inner}->_pvt_multiretval(@_); }
sub doeval           { my $self=shift; $self->{inner}->doeval(@_) }
sub xxxtest           { my $self=shift; $self->{inner}->xxxtest(@_) }

package main;
use Spreadsheet::Edit::Log qw/nearest_call abbrev_call_fn_ln_subname/;
my $myFILE_basename = basename(__FILE__);

my $obj = Outer->new("obj");
my $obj2 = Outer->new("obj2");

my $checklog_callback_lno = (__LINE__) + 4;
sub checklog(&$;$$) {
  my ($code, $exptail, $test_label, $nohead) = @_;
  my ($out, $err) = my_capture {
                      $code->()
  };
  my ($file, $lno) = (caller(0))[1,2];
  $file = basename($file);
  my $exphead = $nohead ? "" : ">[${file}:${lno}] ";
  my $exp = ref($exptail) ? qr/\A\Q$exphead\E$exptail\n\z/
                          : $exphead.$exptail."\n";
  chomp( $test_label ||= $exp );
  #my ($out, $err) = my_capture { $code->() };
  @_ = ($err, $exp, $test_label);
  unless ($out eq "") {
    &Test2::V0::like;
    warn "STDOUT was not empty:\n$out";
    @_ = (0, "STDOUT should be empty");
    goto &Test2::V0::ok;
  }
  goto &Test2::V0::like;  # show caller's line number
}
note dvis '### $checklog_callback_lno\n';

#### Test nearest_call & abbrev_call_fn_ln_subname used directly ####

sub _inner {
  my ($pkg,$fn,$lno,$subname) = @{ nearest_call() };
  my @abbr = abbrev_call_fn_ln_subname();
  # Avoid 'warn' here -- I suspect some test harnesses make it Carp::Always
  print STDERR ivis '$pkg $lno $subname abbrev=@abbr\n';
}
sub interm {
  &_inner;
}
sub foo {
  &interm;
}
checklog { &interm }
  '"main" '.(__LINE__-1)." \"main::interm\" abbrev=(\"$myFILE_basename\",".(__LINE__-1).',"interm")',
  "nerest_call() / abbrev_call_fn_ln_subname",
  "NOHEAD";

#### Test log_call using our custom fmt_object callback ####

checklog { $obj->meth_noretval; } qr/Inner.*\[obj\].meth_noretval/ ;
checklog { $obj2->meth_noretval } qr/Inner.*\[obj2\].meth_noretval/ ;
checklog { $obj2->meth_noretval("A") } '.meth_noretval "A"' ;
checklog { $obj->meth_noretval(0,"0") } qr/Inner.*\[obj\].meth_noretval 0,"0"/;
checklog { $obj->meth_noretval([3..7]) } '.meth_noretval [3,4,5,6,7]' ;

checklog { $obj->meth_1retval } '.meth_1retval() ==> 42' ;
checklog { $obj->meth_1retval("onearg") } '.meth_1retval "onearg" ==> 42' ;
checklog { $obj->meth_1retval("two", "args") } '.meth_1retval "two","args" ==> 42' ;

# log_call \@_, [\"z:",\"","ab",\"ccc",\"",99,{aaa=>100},\"there it was",99];
checklog { &Inner::func1("directcall") } 'func1 "directcall" ==> z:,"ab"ccc,99,{aaa => 100}there it was99' ;

#### Test log_call using the fallback default fmt_object ####
delete $Inner::SpreadsheetEdit_Log_Options{fmt_object};

my $obj_xx = Outer->new("obj_xx");
checklog { $obj_xx->meth_noretval; } qr/<\d{3,}:[\da-fA-F]{3,}>\.meth_noretval/;
checklog { $obj_xx->meth_noretval; } qr/\.meth_noretval/;
checklog { $obj2->meth_1retval; } qr/<\d{3,}:[\da-fA-F]{3,}>\.meth_1retval\(\) ==> 42/;

#### Test btw and friends ####

my $have_color_terminal = do{
  my ($outerr, $exitstat) = $ENV{TERM} && my_capture_merged { system("tput", "sgr0") };
  if (!$ENV{TERM} || $exitstat != 0 || $outerr !~ /^\033.*m$/) {
    note "Terminal is unsuitable for tput color escapes\n",
         dvis '$ENV{TERM} $exitstat $outerr\n';
    0
  } else {
    1
  }
};

foreach my $import_arg (":nocolor",":color") {
  Spreadsheet::Edit::Log->import($import_arg);
  my $color_re = "";
  if ($import_arg eq ':color') {
    $color_re = qr/\033.*?m/;
    unless ($have_color_terminal) {
      note "Skipping btw tests with :color\n";
      next
    }
  }

  { my $out = my_capture_merged { Inner::btw("Test btw call ($import_arg)") }; note $out; };


  { my $obj = Outer->new("btw_test"); # equivalent to btwN 0,...
    my $explno = (__LINE__)+1;
    checklog { $obj->doeval('btw "FOO"') }  qr{
                # NOTE: The custom prefix is 'l=$lno/fn=$fname/pk=$pkg/p=$package'
                l=1/fn=\(eval\ \d+\)/pk=Inner/p=Inner:\ ${color_re}FOO${color_re}
             }sx, "btw test (${import_arg})", "NOHEAD" ;
  }
  { my $obj = Outer->new("btwN3_test");
    my $explno = (__LINE__)+1;
    checklog { $obj->doeval('btwN 3,"FOO"') }  qr{
                # something like 141/90_Log.t/main/main««: FOO
                l=${explno}/fn=$myFILE_basename/pk=main/p=main[^\r\n\/]*:\ ${color_re}FOO${color_re}
             }sx, "btwN 3,... (${import_arg})", "NOHEAD" ;
  }
  { my $obj = Outer->new("btwNslash2_test");
    my $explno = (__LINE__)+1;
    checklog { $obj->doeval('btwN \2, "FOO"') }  qr{
                # Note: Now the outermost frame is first
                l=\d+/fn=$myFILE_basename/pk=Inner/p=Inner\
                .*?
                l=1/fn=\(eval\ \d+\)/pk=Inner/p=Inner:\ ${color_re}FOO${color_re}
             }sx,
             "btwN \\2,... (${import_arg})",
             "NOHEAD"
              ;
  }
  { my $obj = Outer->new("btwbt_test");
    my $explno = (__LINE__)+1;
    checklog { $obj->doeval('btwbt "FOO"') }  qr{
                l=$explno/fn=$myFILE_basename/pk=main/p=main\
                .* # Capture::Tiny stuff
                l=$checklog_callback_lno/fn=$myFILE_basename/pk=main/p=main\
                .*?
                l=\d+/fn=$myFILE_basename/pk=Outer/p=Outer\
                .*?
                l=1/fn=\(eval\ \d+\)/pk=Inner/p=Inner:\ ${color_re}FOO${color_re}
             }sx,
             "btwbt test (${import_arg})",
             "NOHEAD"
              ;
  }
}

done_testing();
exit 0;

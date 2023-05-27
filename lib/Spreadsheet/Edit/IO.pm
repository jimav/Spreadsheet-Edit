#; License: http://creativecommons.org/publicdomain/zero/1.0/
# (CC0 or Public Domain).  To the extent possible under law, the author,
# Jim Avera (email jim.avera at gmail dot com) has waived all copyright and
# related or neighboring rights to this document.  Attribution is requested
# but not required.
use strict; use warnings FATAL => 'all'; use utf8;
use feature qw(say state lexical_subs current_sub);
no warnings qw(experimental::lexical_subs);

package Spreadsheet::Edit::IO;
# VERSION from Dist::Zilla::Plugin::OurPkgVersion
# DATE from Dist::Zilla::Plugin::OurDate

# This module is derived from the old never-released Text:CSV::Spreadsheet

use Exporter 'import';
our @EXPORT_OK = qw(@sane_CSV_read_options @sane_CSV_write_options
                    cx2let let2cx cxrx2sheetaddr convert_spreadsheet OpenAsCsv
                    sheetname_from_spec filepath_from_spec
                    form_spec_with_sheetname
                   );

# TODO: Provide "known_attributes" function ala Text::CSV::known_attributes()

use version ();
use Carp;
sub oops(@) { @_=("\n".__PACKAGE__." oops:\n",@_,"\n"); goto &Carp::confess }
sub btw(@) { local $_=join("",@_); s/\n\z//s; warn((caller(0))[2].": $_\n"); }

use File::Copy ();
use File::Copy::Recursive ();

use Path::Tiny qw/path/;

# Path::Tiny OBVIATES NEED for many but we still need this
use File::Spec::Functions qw/devnull tmpdir/;

# Still sometimes convenient...
use File::Basename qw(basename dirname);

use File::Which qw/which/;
use Guard qw(guard scope_guard);
use Fcntl qw(:flock :seek);
use List::Util qw/none all notall first/;
use Encode qw(encode decode);
# DDI 5.015 is needed for 'qshlist'
use Data::Dumper::Interp qw/vis visq dvis ivis avis qsh qshlist u/;
use File::Glob qw/bsd_glob GLOB_NOCASE/;
use Digest::MD5 qw/md5_base64/;

use Spreadsheet::Edit::Log qw/log_call fmt_call log_methcall fmt_methcall/;
our %SpreadsheetEdit_Log_Options = (
  is_public_api => sub{
    # hide calls
     $_[1][3] =~ /(?:::|^)[a-z][^:]*$/
  },
);

my $progname = path($0)->basename;

# A private Libre/Open Office profile dir is needed to avoid conflicts
# with interactive sessions, see
# https://ask.libreoffice.org/en/question/290306/how-to-start-independent-lo-instance-process
#
# We use a persistent profile dir shared among processes for a given user
# (actually one for each unique external tool which needs one).
# Sharing is okay because we get an exclusive lock before actually using it.
state $profile_parent_dir = do{ # also used for lockfile
  my $euid = $>;
  my $user = $euid; # getpwuid($euid) // $euid;
  (my $dname = __PACKAGE__."_${user}_profileparent") =~ s/::/-/g;
  (my $path = path(File::Spec->tmpdir)->child($dname))->mkpath;
  $path # Path::Tiny
};
sub _get_tool_profile_dir($$) {
  my ($opts, $tool_path) = @_;
  my $fingerprint = _file_fingerprint($tool_path);
  my $toolname = path($tool_path)->basename(qw/\.(exe|cmd|bat)$/);
  my $path = $profile_parent_dir->child("${toolname}_$fingerprint");
  $path->mkpath;
  $path
}

# Prevent concurrent document conversions.
# LO & OO can't handle concurrent access to the same profile.
sub _get_exclusive_lock($) { # returns lock object
  my $opts = shift;
  my $lockfile_path = $profile_parent_dir->child("LOCKFILE");
  open my $lock_fh, "+>>", $lockfile_path or die $!;
  chmod 0666, $lock_fh;
  $opts->{lockfile_fh} = $lock_fh;
  if (! flock($lock_fh, LOCK_EX|LOCK_NB)) {
    seek($lock_fh,0,SEEK_SET) or die;
    (my $owner = do{ local $/; <$lock_fh> }) =~ s/\s*\z//s;
    {
      last unless $owner =~ /pid (\d+)/ && (my @s = stat("/proc/$1"));
      last unless my @pw = getpwuid($s[4]);
      $owner = $pw[0]." ".$owner; # user name
    }
    warn ">> ($$) Waiting for exclusive lock owned by $owner",
         " to convert spreadsheet...\n";
    flock($lock_fh, LOCK_EX) or die "flock: $!";
  }
  print $lock_fh "pid $$ ($progname)\n"; # always appends
}
sub _release_lock($) {
  my $opts = shift;
  my $fh = delete($opts->{lockfile_fh}) // oops;
  truncate($fh,0);
  flock($fh, LOCK_UN) or die "flock UN: $!";
  close $fh;
}

# Libre Office text converter "charset" numbers
my %LO_charsets = (
  'WINDOWS1252' => 1, 'WINLATIN1' => 1,
  'APPLEWESTERN' => 2,
  'DOS/OS2437' => 3,
  'DOS/OS2850' => 4,
  'DOS/OS2860' => 5,
  'DOS/OS2861' => 6,
  'DOS/OS2863' => 7,
  'DOS/OS2865' => 8,
  'SYSTEM' => 9, 'SYSTEMDDEFAULT' => 9,
  'SYMBOL' => 10,
  'ASCII' => 11,
  'ISO88591' => 12,
  'ISO88592' => 13,
  'ISO88593' => 14,
  'ISO88594' => 15,
  'ISO88595' => 16,
  'ISO88596' => 17,
  'ISO88597' => 18,
  'ISO88598' => 19,
  'ISO88599' => 20,
  'ISO885914' => 21,
  'ISO885915' => 22,
  'OS2737' => 23,
  'OS2775' => 24,
  'OS2852' => 25,
  'OS2855' => 26,
  'OS2857' => 27,
  'OS2862' => 28,
  'OS2864' => 29,
  'OS2866' => 30,
  'OS2869' => 31,
  'WINDOWS874' => 32,
  'WINDOWS1250' => 33, 'WINLATIN2' => 33,
  'WINDOWS1251' => 34,
  'WINDOWS1253' => 35,
  'WINDOWS1254' => 36,
  'WINDOWS1255' => 37,
  'WINDOWS1256' => 38,
  'WINDOWS1257' => 39,
  'WINDOWS1258' => 40,
  'APPLEARABIC' => 41,
  'APPLECENTRALEUROPEAN' => 42,
  'APPLECROATIAN' => 43,
  'APPLECYRILLIC' => 44,
  'APPLEDEVANAGARI' => 45,
  'APPLEFARSI' => 46,
  'APPLEGREEK' => 47,
  'APPLEGUJARATI' => 48,
  'APPLEGURMUKHI' => 49,
  'APPLEHEBREW' => 50,
  'APPLEICELANDIC' => 51,
  'APPLEROMANIAN' => 52,
  'APPLETHAI' => 53,
  'APPLETURKISH' => 54,
  'APPLEUKRAINIAN' => 55,
  'APPLECHINESESIMPLIFIED' => 56,
  'APPLECHINESETRADITIONAL' => 57,
  'APPLEJAPANESE' => 58,
  'APPLEKOREAN' => 59,
  'WINDOWS932' => 60,
  'WINDOWS936' => 61,
  'WINDOWSWANSUNG949' => 62,
  'WINDOWS950' => 63,
  'SHIFTJIS' => 64,
  'GB2312' => 65,
  'GBT12345' => 66,
  'GBK' => 67, 'GB231280' => 67,
  'BIG5' => 68,
  'EUCJP' => 69,
  'EUCCN' => 70,
  'EUCTW' => 71,
  'ISO2022JP' => 72,
  'ISO2022CN' => 73,
  'KOI8R' => 74,
  'UTF7' => 75,
  'UTF8' => 76,
  'ISO885910' => 77,
  'ISO885913' => 78,
  'EUCKR' => 79,
  'ISO2022KR' => 80,
  'JIS0201' => 81,
  'JIS0208' => 82,
  'JIS0212' => 83,
  'WINDOWSJOHAB1361' => 84,
  'GB18030' => 85,
  'BIG5HKSCS' => 86,
  'TIS620' => 87,
  'KOI8U' => 88,
  'ISCIIDEVANAGARI' => 89,
  'JAVAUTF8' => 90,
  'ADOBESTANDARD' => 91,
  'ADOBESYMBOL' => 92,
  'PT154' => 93,
  'UCS4' => 65534,
  'UCS2' => 65535,
);
=for Pod::Coverage _name2LOcharsetnum
=cut
sub _name2LOcharsetnum($) {
  my ($enc) = @_;
  local $_ = uc $enc;
  while (! $LO_charsets{$_}) {
    # successively remove - and other special characters
    s/\W//a or croak "Unknown encoding name '$enc'";
  }
  $LO_charsets{$_}
}

# convert between 0-based index and spreadsheet column letter code.
# Default argument is $_
sub cx2let(_) {
  my $cx = shift;
  my $ABC="A"; ++$ABC for (1..$cx);
  return $ABC
}
sub let2cx(_) {
  my $ABC = shift;
  my $n = ord(substr($ABC,0,1,"")) - ord('A');
  while (length $ABC) {
    my $letter = substr($ABC,0,1,"");
    $n = (($n+1) * 26) + (ord($letter) - ord('A'));
  }
  return $n;
}
sub cxrx2sheetaddr($$) { # (1,99) -> "B100"
  my ($cx, $rx) = @_;
  return cx2let($cx) . ($rx + 1);
}

=for Pod::Coverage cxrx2sheetaddr oops btw
=cut

our @sane_CSV_read_options = (
  # Text::CSV pod says to not specify 'eol' to allow embedded newlines,
  # and to automatically handle "\n", "\r", or "\r\n".
  #eol         => $/,
  binary      => 1,       # Allow reading embedded newlines & unicode etc.
  sep_char    => ",",
  quote_char  => '"',
  escape_char => '"',     # Embedded "s appear as ""
  allow_whitespace => 0,  # Preserve leading & trailing white space
  auto_diag   => 2,       # die on errors
);
our @sane_CSV_write_options = (
  eol         => $/,      # Necessary when WRITING csv files
  binary      => 1,
  sep_char    => ",",
  quote_char  => '"',
  escape_char => '"',     # Embedded "s appear as ""
  allow_whitespace => 0,  # Preserve leading & trailing white space
  auto_diag   => 2,       # die on errors
);

my %Saved_Sigs;
sub _sighandler {
  if (! $Saved_Sigs{$_[0]} or $Saved_Sigs{$_[0]} eq 'DEFAULT') {
    # The user isn't catching this, so the process will abort without
    # running destructors: Call exit instead
    warn "($$)".__PACKAGE__." caught signal $_[0], exiting\n";
    Carp::cluck "($$)".__PACKAGE__." caught signal $_[0], exiting\n";
    exit 1;
  }
  $SIG{$_[0]} = $Saved_Sigs{$_[0]};
  kill $_[0], $$;
  oops "Default (or user-defined) sig $_[0] action was to ignore!";
}
sub _signals_guard() {
  %Saved_Sigs = ( map{ ($_ => ($SIG{$_} // undef)) } qw/HUP INT QUIT TERM/ );
  $SIG{HUP} = $SIG{INT} = $SIG{QUIT} = $SIG{TERM} = \&_sighandler;
  return guard { @SIG{keys %Saved_Sigs} = (values %Saved_Sigs) }
}

# Create a probably-unique fingerprint for a particular file
sub _file_fingerprint($) {
  my $path = shift;
  my $ctx = Digest::MD5->new;
  $ctx->add($_) for((stat($path))[0,1,9]); # dev,ino,mtime
  substr($ctx->b64digest,0,6)
}

# Find LibreOffice, or failing that OpenOffice
our $OLpath_answer = $ENV{SPREADSHEET_EDIT_LOPATH};
sub _openlibre_path() {
  return $OLpath_answer if $OLpath_answer;
  unless ($ENV{SPREADSHEET_EDIT_IGNPATH}) {
    foreach my $short_name (qw(libreoffice loffice localc)) {
      if ($OLpath_answer = which($short_name)) { return $OLpath_answer }
    }
  }
  # Search for local/isolated LibreOffice (or OO) installations, which are
  # the result of unpacking from a .deb or other archive into a non-standard 
  # location.  The resulting structure is
  #    <somedir>/opt/{libreoffice,openoffice}*/program/soffice
  # where <somedir> may be a full version number, and the * may be a major
  # version number (e.g. "libreoffice7.5", "openoffice4").
  my sub _cmp_subpaths($$) {
    my ($sp1, $sp2) = @_;
    oops     if !defined($sp1);
    return 1 if !defined($sp2);
    # Use longest version in the (sub-)path, e.g. "4.4.1/opt/openoffice4/..."
    my (@v1) = sort { length($a) <=> length($b) } ($sp1 =~ /(\d[.\d]*)/g);
    my (@v2) = sort { length($a) <=> length($b) } ($sp2 =~ /(\d[.\d]*)/g);
    version->parse($v1[-1]//0) <=> version->parse($v2[-1]//0)
  }
  # I tried just doing File::Glob::bsd_glob('/*/*/*/opt/libre*/program') but 
  # it silently failed even though the same glob works from the shell. Mmff...
  my %results;
  $ENV{SPREADSHEET_EDIT_NOLOSEARCH} or
  File::Find::find(
    { wanted => sub{
        # Undef fullname OR invalid "_" filehandle implies a broken symlink, 
        #   see https://github.com/Perl/perl5/issues/21122
        # Zero size implies /proc or something similar; do not enter.
        # File::Find::fullname unreadable implies followed link to inaccessable 
        # (The initial "_" stat may be invalid, so "-l _" is useless)
        $! = 0;
        if (
            !defined($File::Find::fullname) # broken link, per docs
            || (! -r _) || (! -x _) # unreadable item or invalid "_" handle
                                 # https://github.com/Perl/perl5/issues/21122
            || (stat(_))[7] == 0 # zero size ==> /proc or similar
            || ! -r $File::Find::fullname # presumably a symlink to unreadable
            || ! -x _                     # or unsearchable dir
           ) {
          $File::Find::prune = 1;
          return 
        }
        return unless -d _;
        # Maximum depth: /*/*/<unpackparent>/opt/libreofficeXXX/program/
        my $depth = scalar(() = m#(/)#g);
        if (basename($_) eq "opt") {
          my $prefix = path($_)->parent->stringify;
          for my $o_l (qw/libre open/) {
            if (my $path = ( sort +bsd_glob("$_/${o_l}*/program/soffice", 
                                            GLOB_NOCASE) )[-1]) {
              (my $subpath = $path) =~ s/^\Q${prefix}\E// or oops;
              if (_cmp_subpaths($subpath, $results{$o_l}{subpath}) >= 0) {
                @{$results{$o_l}}{qw/path subpath/} = ($path, $subpath);
              }
            }
          }
        }
        elsif ($depth == 4) {
          $File::Find::prune = 1; 
          return;
        }
        elsif ($depth > 4) { oops dvis '$depth $_' }
      }, 
      follow_fast => 1,
      follow_skip => 2,
      dangling_symlinks => 0,
      no_chdir => 1
    },
    "/", (defined($ENV{HOME}) ? $ENV{HOME} : ())
  );
  $OLpath_answer = path(
     $results{libre}{path} // $results{open}{path} 
       // (!$ENV{SPREADSHEET_EDIT_IGNPATH} && which("soffice")) # installed OO?
       // croak "Can not find LibreOffice or OpenOffice"
     )->realpath->stringify
}#_openlibre_path

sub _openlibre_features() {
  state $hash;
  return $hash if defined $hash;
  my $prog = _openlibre_path() // return(($hash={ available => 0 }));
  my ($s) = (qx/$prog --version/ =~ /Libre.*? (\d+\.\d+\.\w+)/);
  confess "$prog --version DID NOT WORK" unless $s;
  my $version = version->parse("v".($s//"0.1"));
  $hash = {
    available => 1,
    # LibreOffice 7.2 allows extracting all sheets at once
    allsheets => ($version >= version->parse("v7.2")),
    # ...but not yet extracting a single sheet by name.
    # https://bugs.documentfoundation.org/show_bug.cgi?id=135762#c24
    named_sheet => 0,
  }
}
sub _openlibre_supports_allsheets() { _openlibre_features()->{allsheets} }
sub _openlibre_supports_named_sheet() { _openlibre_features()->{named_sheet} }

sub _ssconvert_features() { return { availble => 0 } } # TODO add back?
sub _ssconvert_supports_allsheets() { _ssconvert_features()->{allsheets} }
sub _ssconvert_supports_named_sheet() { _ssconvert_features()->{named_sheet} }

sub _runcmd($@) {
  my ($opts, @cmd) = @_;
  my $guard = _signals_guard;
  warn "> ",join(" ", map{qsh} @cmd),"\n" if $opts->{verbose};
  my $pid = fork;
  if ($pid == 0) { # CHILD
    if ($opts->{suppress_stderr}) {
      open(STDERR, ">", devnull()) or croak $!;
    }
    if ($opts->{stdout_to_stderr}) {
      open(STDOUT, ">&STDERR") or croak $!;
    }
    elsif ($opts->{suppress_stdout}) {
      open(STDOUT, ">", devnull()) or croak $!;
    }
    exec(@cmd) or print "### exec failed: $!\n";
    die;
  }
  waitpid($pid,0);
  my $r = $?;
  warn "(wait status=$r)\n" if $opts->{verbose};
  return $r;
}

sub _fmt_outpath_contents($) {
  my $outpath = $_[0]->{outpath} // oops;
  return "" unless -d $outpath;
  "\n  outpath contains: "
         .join(", ",map{qsh basename $_} path($outpath)->children);
}

my $tempdir;
sub _create_tempdir_if_needed($) {
  my $opts = shift;
  # Keep a per-process persistent temp directory, deleted at process exit.
  # It contains result files when the user did not specify {outpath},
  # plus a cache of as-yet unrequested sheet .csv files, used when the
  # external tool can only extract all sheets, not a single sheet by name:
  #
  #              tempdir/
  #                <ifbase>_<sig>.xlsx etc. # single file returned to user
  #                <ifbase>_<sig>/*.csv     # directory returned to user
  #                <ifbase>_<sig>_csvcache/*.csv
  # 
  # <ifbase> is derived from the intput file name, and <sig> is a fingerprint 
  # based on input file's dev, inode, and modification timestamp.
  #
  $tempdir //= do{
    #(my $template = __PACKAGE__."_XXXXX") =~ s/::/-/g;
    #Path::Tiny->tempdir($template)
    my $pid = $$;
    my $euid = $>;
    my $user = getpwuid($euid) // $euid;
    (my $dname = __PACKAGE__."_${user}_${pid}") =~ s/::/-/g;
    (my $path = path(File::Spec->tmpdir)->child($dname))->mkpath;
    $path
  };
}
END{ $tempdir->remove_tree if $tempdir; }

# Compose {tempdir}/{ifbase}_<signature>_csvcach 
sub _cachedir($) {
  my $opts = shift;
  $opts->{inpath_fingerprint} 
              //= _file_fingerprint($opts->{inpath_sans_sheet});
  my $dname = $opts->{ifbase}."_".$opts->{inpath_fingerprint}."_csvcache";
  $tempdir->child($dname)
}

sub _convert_using_openlibre($$) {
  my ($opts, $dst) = @_;
  oops unless all{ $opts->{$_} } qw/inpath_sans_sheet cvt_from cvt_to/;
  oops if $opts->{allsheets} && ! _openlibre_supports_allsheets();
  oops if $opts->{sheetname} && ! _openlibre_supports_named_sheet();
  oops if $dst =~ /\.csv$/;
  my $debug = $opts->{debug};

  my $prog = _openlibre_path() // oops;

  my $saved_UserInstallation = $ENV{UserInstallation};
  $ENV{UserInstallation} = "file://"._get_tool_profile_dir($opts, $prog);
  scope_guard {
    if (defined $saved_UserInstallation) {
      $ENV{UserInstallation} = $saved_UserInstallation;
    } else {
      delete $ENV{UserInstallation}
    }
  };

  # The --convert-to argument is "suffix:filtername:filteropts"
  
  # I think (not certain) that we can only specify the encoding of CSV files,
  # either as input or output;  .xlsx and .ods spreadsheets (which are based
  # on XML) could in principle use any encoding internally, but I'm not sure
  # we can control that, nor should anyone ever need to.

  # REFERENCES:
  # https://help.libreoffice.org/7.5/en-US/text/shared/guide/start_parameters.html?&DbPAR=SHARED&System=UNIX
  # http://wiki.openoffice.org/wiki/Documentation/DevGuide/Spreadsheets/Filter_Options
  # https://wiki.documentfoundation.org/Documentation/DevGuide/Spreadsheet_Documents#Filter_Options_for_the_CSV_Filter
  
  state $suf2ofilter = {
    csv  => "Text - txt - csv (StarCalc)",
    txt  => "Text - txt - csv (StarCalc)",
    xls  => "MS Excel 97",
    xlsx => "Calc MS Excel 2007 XML",
  };

  my $ifilter = $opts->{soffice_infilter} //= do{
    my $filter_name = $suf2ofilter->{$opts->{cvt_from}} 
        // croak "Don't know soffice output filter name for $opts->{cvt_from}";
    if ($opts->{cvt_from} eq "csv") {
      #CSV INPUT FORMAT CONTROL UNTESTED as of 2/6/2021
      my $enc = $opts->{input_encoding};
      my $charset = _name2LOcharsetnum($enc); # dies if unknown enc
      my $colformats = "";
      if (my $cf = $opts->{col_formats}) {
        $cf = [split /\//, $cf] if !ref($cf);  #  fmtA/fmtB/...
        for (my $ix=0; $ix <= $#$cf; $ix++) {
          local $_ = $cf->[$ix];
             m#^([123459]|10)$#
          || s#^standard$#1#i
          || s#^text$#2#i
          || s#^M+/D+/Y+$#3#i
          || s#^D+/M+/Y+$#4#i
          || s#^Y+/M+/D+$#5#i
          || s#^ignore$#9#i
          || s#^US.*English$#10#i
          || croak "Unknown format code '$_' in {col_formats}";
          $colformats .= "/" if $colformats;
          $colformats .= "$ix/$_";
        }
      }
      $filter_name.":"
      # Tokens 1-5: FldSep=, TxtDelim='"' Charset FirstLineNum CellFormats
      ."44,34,$charset,1,$colformats"
      # Tokens 6-7: LanguageId QuoteAllTextCells
      .",true" ;
    } 
    else {
      $filter_name
    }
  };

  my $ofilter = $opts->{soffice_outfilter} //= do{
    # OutputFilterName[:paramtoken,paramtoken,...]
    my $filter_name = $suf2ofilter->{$opts->{cvt_to}} 
        // croak "Don't know soffice output filter name for $opts->{cvt_to}";
    if ($opts->{cvt_to} eq "csv") {
      my $enc = $opts->{output_encoding};
      my $charset = _name2LOcharsetnum($enc); # dies if unknown enc
      $filter_name.":"
      # Tokens 1-4: FldSep=, TxtDelim=" Charset FirstLineNum
      ."44,34,$charset,1"
      # Token 5: Cell format codes, separated by / 
      #  1=Std 2=Text 3=MM/DD/YY 4=DD/MM/YY 5=YY/MM/DD 6-8=?? 
      #  9=ignore field (do not import),10=US-English (=> 3.14 not 3,14)
      #  (I'm guessing 1 is like 10 but using current or specified language)
      .","
      # Token 6: Language identifier (uses Microsoft lang ids)
      #   1033 means US-English (omitted => use UI's language)
      .","
      # Token 7: QuoteAllTextCells
      # *** true will "quote" even single-bareword cells, which looks
      # *** bad and makes t/ tests messier.  OTOH quotes provide information
      # *** that such cells were not numbers or dates, etc.  
      # *** So I'm now using "true" to always quote.
      .",true"
      # Token 8: DetectSpecialNumbers"
      .","
      # Token 9: "Save cell contents as shown"
      .",".($opts->{raw_values} ? "false" : "true")
      # Token 10: "Export cell formulas"
      .",false"
      # Token 11: not used during export
      .","
      # Token 12: (LO 7.2+) sheet selections:
      #   0 or absent => the "first" sheet
      #   1-N => the Nth sheet (arrgh, can not specify name!!)
      #   -1 => export all sheets to files named filebasenamne.Sheetname.csv
      .",".($opts->{allsheets} ? -1 : die("add named-sheet support here"))
    }
    else {
      $filter_name
    }
  };

  # We can only control the output directory path, not the name of
  # an individual result file.  If $dst is a directory then the result 
  # could theoretically output into it directly, but instead we always 
  # output to an ephemeral temp directory and then move the results to $dst
  #
  # With 'allsheets' the resulting files must be renamed to conform to our
  # external API (namely SHEETNAME.csv).
  #
  # ERROR DETECTION: As of LO 7.5 we always get zero exist status and the 
  # only way to detect errors is to notice that no files were written.
  # https://bugs.documentfoundation.org/show_bug.cgi?id=155415
  #
  my $tdir = Path::Tiny->tempdir(path($dst)->basename."_XXXXX");
  # will be deleted when $dirpath goes out of scope
  
  my @cmd = ($prog, "--headless", "--invisible",
                    $ifilter ? ("--infilter=$ifilter") : (),
                    "--convert-to", 
                      $opts->{cvt_to}.($ofilter ? ":$ofilter" : ""),
                    "--outdir", $tdir,
                    $opts->{inpath_sans_sheet});

  $opts->{stdout_to_stderr} = 1;       # send "convert..." message to stderr
  $opts->{suppress_stderr} = !$debug;  # and suppress it unless tracing

  my $cmdstatus = _runcmd($opts, @cmd);

  if ($cmdstatus != 0) {
    # This should never happen, see ERROR DETECTION above
    croak "($$) Conversion of '$opts->{inpath}' to $opts->{cvt_to} failed\n";
  }
  
  my @result_files = path($tdir)->children;
  btw dvis '>> @result_files' if $debug;
  if (@result_files == 0) {
    croak $opts->{inpath_sans_sheet}." is missing or unreadable\n"
      unless -r $opts->{inpath_sans_sheet};
    croak "Something went wrong, ",path($prog)->basename," produced no output\n"
  }

  if ($opts->{allsheets}) {
    # Rename files to match our API (omit the spreadsheetbasename- prefix)
    foreach (@result_files) {
      my $dir  = $_->dirname;
      my $base = $_->basename;
      (my $newbase = $base) =~ s/^$opts->{ifbase}-// or oops;
      my $newpath = path($dir)->child($newbase);
      btw ">> Renaming $_ -> $newpath\n" if $debug;
      rename ($_, $newpath) or oops "$!";
      $_ = $newpath; # update @result_files
    }
  }

  # Move the results to $dst
  if (-e $dst) {
    croak "$dst must be a directory if it pre-exists\n" unless -d $dst;
    btw ">> Moving results -> $dst\n" if $debug;
    foreach (@result_files) { 
      btw ">>> move $_ -> $dst" if $debug;
      File::Copy::move($_, $dst) 
    }
    btw ">> Now $dst contains: ",avis($dst->children) if $debug;
  } else {
    if ($opts->{allsheets}) {
      btw ">> dirmove $tdir -> $dst\n" if $debug;
      rename($tdir, $dst) or File::Copy::dirmove($tdir, $dst);
    } else {
      croak "Expecting only one result file, not @result_files"
        if @result_files > 1;
      btw ">> move $result_files[0] -> $dst\n" if $debug;
      File::Copy::move($result_files[0], $dst);
    }
  }
}

sub _convert_using_ssconvert($$) {
  my ($opts, $dst) = @_;
  confess "Deprecated with extreme prejudice"; # no longer supported
##
##  foreach (qw/inpath cvt_to /)
##    { oops "missing opts->{$_}" unless exists $opts->{$_} }
##
##  my $eff_outpath = $opts->{outpath};
##  if (my $prog=which("ssconvert")) {
##    my $enc = _get_encodings_from_opts($opts);
##    $enc //= "UTF-8"; # default
##    my @options;
##    if ($opts->{cvt_to} eq "csv") {
##      push @options, '--export-type=Gnumeric_stf:stf_assistant';
##      my @dashO_terms = ("format=preserve", "transliterate-mode=escape");
##      push @dashO_terms, "charset='${enc}'" if defined($enc);
##      if ($opts->{sheetname}) {
##        push @dashO_terms, "sheet='$opts->{sheetname}'";
##      }
##      if ($opts->{allsheets}) {
##        #If both {allsheets} and {sheetname} are specified, only a single
##        # .csv file will be in the output directory
##        croak "'allsheets' option: 'outpath' must specify an existing directory"
##          unless -d $eff_outpath;
##        $eff_outpath = catfile($eff_outpath, "%s.csv");
##        push @options, "--export-file-per-sheet";
##      }
##      elsif ($opts->{sheetname}) {
##        # handled above
##      }
##      else {
##        # A backwards-incompatible change to ssconvert stopped extracting
##        # the "current" sheet by default; now all sheets are concatenated!
##        # See https://gitlab.gnome.org/GNOME/gnumeric/issues/461
##        # ssconvert verison 1.12.45 supports a new "-O active-sheet=y" option
##  ## PORTABILITY BUG: Redirection syntax will not work on windows
##        my ($ssver) = (qx/ssconvert --version 2>&1/ =~ /ssconvert version '?(\d[\d\.]*)/);
##        if (version::is_lax($ssver) && version->parse($ssver) >= v1.12.45) {
##          push @dashO_terms, "active-sheet=y";
##        } else {
##          croak("Due to an ssconvert bug, a sheetname must be given.\n",
##                "(for more information, see comment at ",__FILE__,
##                " near line ", (__LINE__-10), ")\n");
##        }
##      }
##      push @options, '-O', join(" ",@dashO_terms);
##    }
##    elsif ($opts->{cvt_to} eq 'xlsx') {
##      @options = ('--export-type=Gnumeric_Excel:xlsx2');
##    }
##    elsif ($opts->{cvt_to} eq 'xls') {
##      @options = ('--export-type=Gnumeric_Excel:excel_biff8'); # M'soft Excel 97/2000/XP
##    }
##    elsif ($opts->{cvt_to} =~ /^od/) {
##      @options = ('--export-type=Gnumeric_OpenCalc:odf');
##    }
##    elsif ($eff_outpath =~ /\.[a-z]{3,4}$/) {
##      # let ssconvert choose based on the output file suffix
##    }
##    else {
##      croak "unrecognized cvt_to='".u($opts->{cvt_to})."' and no outpath suffix";
##    }
##
##    my $eff_inpath = $opts->{inpath};
##    if ($opts->{sheetname} && $opts->{inpath} =~ /.csv$/i) {
##      # Control generated sheet name by using a symlink to the input file
##      # See http://stackoverflow.com/questions/22550050/how-to-convert-csv-to-xls-with-ssconvert
##      my $td = catdir($tempdir // oops, "Gnumeric");
##      remove_tree($td); mkdir($td) or die $!;
##      $eff_inpath = catfile($td, $opts->{sheetname});
##      symlink $opts->{inpath}, $eff_inpath or die $!;
##    }
##    my @cmd = ($prog, @options, $eff_inpath, $eff_outpath);
##
##    my $suppress_stderr = !$opts->{debug};
##    if (0 != _runcmd({%$opts, suppress_stderr => $suppress_stderr}, @cmd)) {
##      # Before showing a complicated ssconvert failure with backtrace,
##      # check to see if the problem is just a non-existent input file
##      { open my $dummy_fh, "<", $eff_inpath or croak "$eff_inpath : $!"; }
##      my $failmsg = "($$) Conversion of '$opts->{inpath}' to $eff_outpath failed\n"."cmd: ".qshlist(@cmd)."\n";
##      if ($suppress_stderr) {  # repeat showing all output
##        if (0 == _runcmd({%$opts, suppress_stderr => 0}, @cmd)) {
##          warn "Surprise!  Command failed the first time but succeeded on 2nd try!\n";
##        }
##        croak $failmsg;
##      }
##    }
##    elsif (! -e $opts->{outpath}) {
##      croak "($$) Conversion SILENTLY failed\n(using $prog)\n",
##            "  cmd: ",qshlist(@cmd),"\n"
##            ;
##    }
##    return ($enc)
##  }
##  else {
##    croak "Can not find ssconvert to convert '$opts->{inpath}' to $opts->{cvt_to}\n",
##        "To install ssconvert: sudo apt-get install gnumeric\n";
##  }
}

# Extracts |||SHEETNAME or !SHEETNAME or [SHEETNAME] from a path+sheet
# specification, if present.
# (Lots of historical compatibility issues...)
# In scalar context, returns SHEETNAME or undef.
# INTERNAL USE ONLY: In array  context, returns (filepath, SHEETNAME or undef)
sub sheetname_from_spec($) {
  my $spec = shift;
  local $_;
  my $p = path($spec);
  my $parent = $p->parent;
  my ($base,$sn) = ($p->basename =~ /^(.*) (?| \|\|\|([^\!\[\|]+)$
                                             | \!([^\!\[\|]+)$
                                             | \[([^\[\]]+)\]$
                                           )/x);
  wantarray ? ($parent->child($base//$p->basename)->stringify, $sn) : $sn
}
sub filepath_from_spec($) {
  my ($path, undef) = sheetname_from_spec($_[0]);
  $path
}
#Tester
#foreach ("", "/a!b/c", "/a!b/c!sheet1", "/a/b/c[sheet2]", "/a/b/c[bozo]d.xls",
#        ) {
#  foreach($_, basename($_)) {
#    my ($fp,$sn) = sheetname_from_spec($_);
#    use open ':std', ':locale';
#    warn ivis '# $_ →  $fp $sn\n';
#    my $sn2 = sheetname_from_spec($_);
#    die "bug" unless u($sn) eq u($sn2);
#  }
#}
#die "TEX";

# Construct a file + sheetname spec in the preferred form for humans to read
# If sheetname is undef, just return the file path
sub form_spec_with_sheetname($$) {
  my ($filespec, $sheetname) = @_;
  my $embedded_sheetname = sheetname_from_spec($filespec);
  croak "conflicting embedded and separate sheetnames given"
    if $embedded_sheetname && $sheetname && $embedded_sheetname ne $sheetname;
  $sheetname //= $embedded_sheetname;
  my $filepath = filepath_from_spec($filespec);
  $sheetname ? "${filepath}[${sheetname}]" : $filepath
  #$sheetname ? "${filepath}|||${sheetname}" : $filepath
}

our $default_input_encodings = "UTF-8,UTF-16BE,UTF-16LE,windows-1252";
our $default_output_encoding = "UTF-8";

# Return digested %opts setting
#   sheetname, inpath_sans_sheet (as Path::Tiny), encoding or default
sub _process_args($;@) {
  confess "fix obsolete call to pass linearized options" 
    if ref($_[0]) eq "HASH";
  my $leading_inpath = ( scalar(@_) % 2 == 1 ? shift(@_) : undef );
  my %opts = (
              cvt_from => "",
              cvt_to => "",
              @_,
              #verbose => 999, tempdir => "/tmp/J",
            );
  if (defined $opts{inpath}) {
    croak "Initial INPATH arg specified as well as inpath => ... in options"
      if defined $leading_inpath;
  } else {
    $opts{inpath} = $leading_inpath // croak "No inpath was specified";
  }
  $opts{verbose}=1 if $opts{debug};

  # inpath or outpath may have "!sheetname" appended (or alternate syntaxes),
  # but may exist only if a separate 'sheetname' option is not specified.
  # Input and output can not both be spreadsheets; one must be a CSV.
  if (exists($opts{sheet})) {
    carp "WARNING: Deprecated 'sheet' option found (use 'sheetname' instead)\n";
    croak "Both {sheet} and {sheetname} specified" if exists $opts{sheetname};
    $opts{sheetname} = delete $opts{sheet};
  }
  { my ($path_sans_sheet, $sheetname, $key);
    for my $thiskey ('inpath', 'outpath') {
      my $spec = $opts{$thiskey} || next;
      my ($pssn, $sn) = sheetname_from_spec($spec);
      if (defined $sn) {
        croak "A sheetname is embeeded in both ",
              "'$thiskey' ($opts{$thiskey}) and '$key' ($opts{$key})\n"
          if $sheetname;
        ($path_sans_sheet, $sheetname, $key) = ($pssn, $sn, $thiskey);
      }
    }
    if ($opts{sheetname}) {
      croak "'sheetname' option conflicts with embedded sheet name\n",
            "   sheetname => ", qsh($opts{sheetname}),"\n",
            "   $key => ", qsh($opts{$key}),"\n"
        if defined($sheetname) && $sheetname ne $opts{sheetname};
    }
    elsif (defined $sheetname) {
      btw "(extracted sheet name \"$sheetname\" from $key)\n"
        if $opts{verbose};
      $opts{sheetname} = $sheetname;
    }
    $opts{inpath_sans_sheet} = path(
      ($key && $key eq 'inpath') ? $path_sans_sheet : $opts{inpath}
    );
  }
  # Input file basename sans any .suffix
  $opts{ifbase} = $opts{inpath_sans_sheet}->basename(qr/\.[^.]+/);

  %opts
}#_process_args

# Extract the of encoding(s) specified in an iolayers string
# Parse iolayers string, returning ($prefix,[encodings],$suffix) 
# For example from ":raw:encodings(utf8,windows-1252):zz" the output
# would be (":raw", [:utf8","windows-1252"], ":zz")
sub _parse_iolayers($) {
  local $_ = (shift) // "";
  /\A(<prefix>.*?)
     (<encspec>:utf8|:encoding\(([^\)]+)\))
     (<suffix>.*?)\z/ or croak "Invalid iolayers spec '$_'\n";
  (my $prefix, $_, my $suffix) = ($+{prefix}, $+{encspec}, $+{suffix});
  /^:(utf8)$/ || /^:encoding\(([^\)]+)\)$/ or oops($_);
  my $enclist = [split /,/, $1]; # comma,separated,list,of,encodings
  ($prefix, $enclist, $suffix);
}

# Detect cvt_to and input_encoding if a csv; 
# Detect cvt_to and set default output_encoding if not specified
sub _determine_enc_tofrom($) {
  my $opts = shift;
  unless ($opts->{cvt_to}) {
    if ($opts->{outpath} && $opts->{outpath} =~ /\.([^.]+)$/) {
      $opts->{cvt_to} = $1;
    }
    croak "'cvt_to' was not specified and can not be intuited from 'outpath'"
      ,dvis('\n### $opts')  ###TEMP
      unless $opts->{cvt_to};
  }
  unless ($opts->{cvt_from}) {
    if ($opts->{inpath_sans_sheet} 
                          && $opts->{inpath_sans_sheet} =~ /\.([^.]+)$/) {
      $opts->{cvt_from} = $1;
      $opts->{cvt_from} =~ s/^\.txt$/.csv/i;
    } 
  }
  # If we don't know what the input format is, or we do know it's CSV but
  # don't know its encoding, examine the file content to figure it out.
  if (!$opts->{cvt_from} || $opts->{cvt_from} eq "csv") {
    my $octets;
    $opts->{input_encoding} //= $default_input_encodings;
    my @enclist = split m#,#, $opts->{input_encoding};
    if (@enclist > 1) {
      $octets = $opts->{inpath_sans_sheet}->slurp_raw;
      for my $enc (@enclist) {
        eval { decode($enc, $octets, Encode::FB_CROAK|Encode::LEAVE_SRC) };
        if ($@) {
           btw "Input encoding '$enc' did not work...($@)\n" if $opts->{debug};
           next;
        }
        btw "Input encoding '$enc' seems to work.\n" if $opts->{debug};
        @enclist = ($enc);
        last
      }
      #croak "Could not detect encoding of $opts->{inpath_sans_sheet}\n" 
      confess "Could not detect encoding of $opts->{inpath_sans_sheet}\n" 
        if @enclist > 1;
    }
    $opts->{input_encoding} = $enclist[0];
    if (!$opts->{cvt_from}) {
      $octets //= $opts->{inpath_sans_sheet}->slurp_raw;
      my $chars = decode($opts->{input_encoding}, $octets, Encode::FB_CROAK);
      # Does it look like a csv file?
      # This is not at all correct, e.g. doesn't know about "quoted" fields,
      # but merely screens for total garbage
      my $min_cols_minus1 = 3 - 1;
      if ($chars =~ /\A(?:.*?,){$min_cols_minus1,}[^,]*[\x{0A}\x{0D}]/s
          ||
          $chars =~ /\A(?:.*?\t){$min_cols_minus1,}[^\t]*[\x{0A}\x{0D}]/s
          ||
          length($octets)==0) {
        $opts->{cvt_from} = 'csv';
      } else {
        croak "Can not detect what kind of file ",qsh($opts->{inpath})," is\n";
      }
    }
  }
  $opts->{output_encoding} //= $default_output_encoding
    if $opts->{cvt_to} eq "csv";
}#_determine_enc_tofrom

sub _tool_extract_all_csvs($$) {
  my ($opts, $destdir) = @_;
  delete local $opts->{sheetname};
  local $opts->{allsheets} = 1;
  if (_openlibre_supports_allsheets()) {
    _convert_using_openlibre($opts, $destdir);
  }
  elsif (_ssconvert_supports_allsheets()) {
    _convert_using_ssconvert($opts, $destdir);
  }
  else { croak "Can't extract 'allsheets'.  Please install LibreOffice 7.2 or newer" }
}

sub _tool_can_extract_one_csv() { 
  _openlibre_supports_named_sheet() || _ssconvert_supports_named_sheet() 
}
sub _tool_extract_one_csv($$) {
  my ($opts, $destpath) = @_;
  confess "should not get here"; # unless we restore ssconvert support
}

# Extract CSVs for every sheet into {outpath} (setting to tmpdir if not preset).
# If cached CSVs are available they are moved into {outpath}/ .
sub _extract_all_csvs($) {
  my $opts = shift;
  my $outpath = _final_outpath($opts);
  $outpath->mkpath; # nop if exists, croaks if conflicts with file

  _tool_extract_all_csvs($opts, $outpath); #logs
}

# Extract a specified sheet into a CSV at {outpath} (defaulting to temp file).
# If a cached CSV is available it is moved to {outpath}.
sub _extract_one_csv($) {
  my $opts = shift;

  my $outpath = _final_outpath($opts);
  $outpath->remove unless -d $outpath;

  my $cachedirpath = _cachedir($opts);
  confess "undef sheetname" unless defined $opts->{sheetname};
  my $fname = $opts->{sheetname}.".csv";
  my $cached_path = $cachedirpath->child($fname);
  if (-e $cached_path) {
    warn "> Moving cached $fname to $outpath\n" if $opts->{verbose};
    File::Copy::move($cached_path, $outpath);
  }
  elsif (_tool_can_extract_one_csv()) {
    _tool_extract_one_csv($opts, $opts->{outpath}); #logs
  } 
  else {
    warn ">>Emulating extract-by-name by extracting all csvs into cache...\n"
      if $opts->{debug};
    $cachedirpath->remove_tree;
    $cachedirpath->mkpath;
    { local $opts->{verbose} = 0;
      #local $opts->{debug} = 0;
      _tool_extract_all_csvs($opts, $cachedirpath);
    }
    __SUB__->($opts);
  }
}

# If {outpath} is not set, set it to a unique temporary path in $tempdir
#
# This is *not* a "tempfile" or "tempdir" object which auto-destructs,
# in fact it does not even exist yet and we don't know here which it will be.  
# Either the user must remove it when they are done with it, or it will 
# be removed when $tempdir is removed a process exit.
#
# Always returns {outpath} as a Path::Tiny object.
sub _final_outpath($) {
  my $opts = shift;
  $opts->{outpath} ? path($opts->{outpath}) : ($opts->{outpath} = do{
     my $base = $opts->{ifbase}
                .($opts->{sheetname} ? "_$opts->{sheetname}" : "");
     # Collisions occur when recursing to emulate Extract-by-name,
     # or if the user repeatedly reads the same exact thing
     state %memory;
     if ($memory{$base}++) {
       $base .= $memory{$base};  # sequence number
     }
     $tempdir->child("${base}.$opts->{cvt_to}")
  });
}

sub convert_spreadsheet(@) {
  # Set inpath_sans_sheet, sheetname, ifbase, etc.
  my %opts = &_process_args;
  btw dvis('>>> convert_spreadsheet %opts\n') if $opts{debug};
  my %input_opts = %opts;

  _get_exclusive_lock(\%opts);
  scope_guard { _release_lock(\%opts); };

  _create_tempdir_if_needed(\%opts);

  _determine_enc_tofrom(\%opts); # intuit cvt_from & cvt_to if possible
  my $input_enc = $opts{input_encoding};
  my $output_enc = $opts{output_encoding};

  croak "Either input or output must be 'csv'\n"
    unless $opts{cvt_from} eq 'csv' || $opts{cvt_to} eq 'csv';
  if ($opts{allsheets}) {
    croak "'allsheets' is allowed only with cvt_to => 'csv'"
      unless ($opts{cvt_to}//"") eq "csv";
    croak "With 'allsheets', a sheet name may not be specified\n"
      if $opts{sheetname};
    croak "With 'allsheets', 'outpath' must be a directory if it exists\n"
      if $opts{outpath} && -e $opts{outpath} && ! -d _;
  }

  my $done;
  if ($opts{cvt_from} eq $opts{cvt_to}) {  # csv to csv
    if (!$opts{allsheets}) {
      if ($input_enc ne $output_enc) {
        # Special case #1: in & out are CSVs but different encodings.
        warn "> Transcoding csv $opts{inpath_sans_sheet}:  $input_enc -> $output_enc\n" 
          if $opts{debug};
        my $octets = $opts{inpath_sans_sheet}->slurp_raw;
        my $chars = decode($input_enc, $octets, Encode::FB_CROAK);
        $octets = encode($output_enc, $chars, Encode::FB_CROAK);
        path(_final_outpath(\%opts))->spew_raw($octets);
        $done = 1;
      } else {
        # Special case #2: No conversion is needed: Just copy the file or 
        #   return the input path itself as the output
        if (defined $opts{outpath}) {
          warn "> No conversion needed, copying to ",qsh($opts{outpath}),"\n"
            if $opts{verbose};
          $opts{inpath_sans_sheet}->copy($opts{outpath});
          $done = 1;
        } else {
          $opts{outpath} = $opts{inpath_sans_sheet};
          warn "> No conversion needed, returning ", qsh($opts{outpath}),"\n"
            if $opts{verbose};
          $done = 1;
        }
      }
    }
    else {
      # Special case #2: <allsheets> with input already a csv:
      #   Leave a symlink to the input in the <outpath> directory.
      croak "transcoding not impl in this situation"
        if ($input_enc ne $output_enc);
      my $outpath = path(_final_outpath(\%opts));
      $outpath->mkpath; # nop if exists, croaks if conflicts with file
      my $dest = $outpath->child( $opts{ifbase}.".csv" );
      symlink($opts{inpath_sans_sheet}, $dest)
        or croak "symlink $opts{inpath_sans_sheet} <-- $dest : $!";
      warn "  No conversion needed! Leaving symlink at ", qsh($dest),"\n"
        if $opts{verbose};
      $done = 1;
    }
  }
  if (! $done) {
    if ($opts{allsheets}) {
      _extract_all_csvs(\%opts);
    } 
    else {
      # Result will be a single file.
      if ($opts{cvt_to} eq "csv") {
        _extract_one_csv(\%opts);
      } else {
        _write_spreadsheet(\%opts);
      }
    }
  }
  my $result = {
    defined($output_enc) ? (encoding => $output_enc):(),
    inpath_sans_sheet => $opts{inpath_sans_sheet}->stringify,
    (map{ ($_ => $opts{$_}) } grep{ defined $opts{$_} }
        qw/outpath cvt_from cvt_to sheetname/)
  };
  log_call [\%input_opts], [$result, \_fmt_outpath_contents($result)]
    if $opts{verbose};

  $result;
}#convert_spreadsheet

# Open as a CSV, intuiting input encoding, converting spreadsheet if necessary.
#
# The :crlf will be used, which translates DOS CR,LF to \n while passing
# *nix bare LF through unmolested.
#
# Input argument(s) are the same as for convert_spreadsheet (except
# outpath may not be specified).
#
# Returns a hash containing the file handle and other information.
sub OpenAsCsv {
  my %opts = (
              (@_ == 1 ? (inpath => $_[0]) : (@_)),
              cvt_to => 'csv',
            );
  # TODO: Rename {path} to {inpath} in all usages and rm this cruft;
  # TODO: Consider renaming {inpath} as {input} bc it can be a filehandle
  #   (massive changes required; in some contexts it must be a path...)
  carp "Obsolete OpenAsCsv usage: Change path to inpath\n"
    if exists($opts{path}) and !$opts{silent};
  $opts{inpath} //= delete $opts{path}; # be compatible with old API 

  my $inpath = delete $opts{inpath};
  croak "OpenAsCsv: missing 'inpath' option\n" unless $inpath;
  croak "OpenAsCsv: outpath may not be specified\n" if $opts{outpath};

  my $h = convert_spreadsheet($inpath, %opts, verbose => $opts{debug});
  oops "sheetname key bug" if exists $h->{sheet};

  my $csvpath = $h->{outpath}; # might be same as {inpath} if already a CSV
  open my $fh, "<", $csvpath or croak "$csvpath : $!\n";
  binmode $fh, ":crlf:encoding(".$h->{encoding}.")" or die "binmode:$!";

  my $r = {
    fh            => $fh,
    csvpath       => $csvpath,
    inpath        => $inpath,
    (map{ exists($h->{$_}) ? ($_ => $h->{$_}) : () }
        qw/inpath_sans_sheet sheetname encoding tempdir raw_values/),
  };

  return $r;
}

1;
__END__

=pod

=head1 NAME

Spreadsheet::Edit::IO - convert between spreadsheet and csv files

=head1 SYNOPSIS

  use Spreadsheet::Edit::IO
        qw/convert_spreadsheet OpenAsCsv
           cx2let let2cx @sane_CSV_read_options @sane_CSV_write_options/;

  # Open a CSV file or result of converting a sheet from a spreadsheet 
  # DEPRECATED? 
  #
  my $hash = OpenAsCsv("/path/to/spreadsheet.odt!Sheet1");  # single-arg form
  my $hash = OpenAsCsv(inpath => "/path/to/spreadsheet.odt", 
                       sheetname -> "Sheet1"); 
  print "Reading ",$hash->{csvpath}," with encoding ",$hash->{encoding},"\n";
  while (<$hash->{fh}>) { ... }

  # Convert CSV to spreadsheet
  $hash = convert_spreadsheet(inpath => "mycsv.csv", cvt_to => "xlsx");
  print "Resulting spreadsheet path is $hash->{outpath}\n";
  
  # Convert a single sheet from a spreadsheet to CSV
  $hash = convert_spreadsheet(inpath => "mywork.xls", sheetname => "Sheet1", 
                              cvt_to => "csv");
  open my $fh, "<", $hash->{outpath};
  binmode $fh, ":encoding(":crlf:encoding(".$hash->{encoding}.")");
  ...

  # Convert all sheets in a spreadsheet to CSV files in a subdir
  $hash = convert_spreadsheet(inpath => "mywork.xls", allsheets => 1,
                              cvt_to => "csv");
  system "ls -l ".$hash->{outpath};  # show resulting .csv files

  # Translate between 0-based column index and letter code (A, B, etc.)
  print "The first column is column ", cx2let(0), " (duh!)\n";
  print "The 100th column is column ", cx2let(99), "\n";
  print "Column BF is index ", let2cx("BF"), "\n";

  # Extract components of "filepath!SHEETNAME" specifiers
  my $path      = filepath_from_spec("/path/to/spreasheet.xls!Sheet1")
  my $sheetname = sheetname_from_spec("/path/to/spreasheet.xls!Sheet1")

  # Parse a csv file with sane options
  my $csv = Text::CSV->new({ @sane_CSV_read_options, eol => $hash->{eol} })
    or die "ERROR: ".Text::CSV->error_diag ();
  my @rows
  while (my $F = $csv->getline( $infh )) {
    push @rows, $F;
  }
  close $infh or die "Error reading ", $hash->csvpath(), ": $!";

  # Write a csv file with sane options
  my $ocsv = Text::CSV->new({ @sane_CSV_write_options })
    or die "ERROR: ".Text::CSV->error_diag ();
  open my $outfh, ">:encoding(utf8)", $outpath
    or die "$outpath: $!";
  foreach (@rows) { $ocsv->print($outfh, $_) }
  close $outfh or die "Error writing $outpath: $!";

=head1 DESCRIPTION

Convert between CSV and spreadsheet files using external programs, plus some utility functions

=head2 $hash = OpenAsCsv INPUT

=head2 $hash = OpenAsCsv inpath => INPUT, sheetname => SHEETNAME, ...

This is a thin wrapper for C<convert_spreadsheet> followed by C<open>
(MAYBE 2B DEPRECATED?)

If a single argument is given it specifies INPUT; otherwise all arguments must
be specified as key => value pairs, and may include any options supported
by C<convert_spreadsheet>.

INPUT may be a csv or spreadsheet workbook path; if a spreadsheet, 
then a single "sheet" is converted, specified by either a !SHEETNAME suffix 
in the INPUT path, or a separate C<< sheetname => SHEETNAME >> option.

The resulting file handle refers to a guaranteed-seekable CSV file; 
this will either be a temporary file (auto-removed at process exit), 
or the original INPUT if it was already a seekable csv file.

RETURNS: A ref to a hash containing the following:

 {
  fh        => the resulting open file handle
  encoding  => the encoding used for the .csv file
  csvpath   => the path {fh} refers to, which might be a temporary file
  sheetname => sheet name if the input was a spreadsheet
 }

=head2 convert_spreadsheet INPUT, cvt_to=>suffix, OPTIONS

=head2 convert_spreadsheet INPUT, cvt_to=>"csv", allsheets => 1, OPTIONS

This converts between CSV and various spreadsheet.  

RETURNS: A ref to a hash containing: 

 {
  outpath   => path to the output file (or directory with 'allsheets')
               (a temporary file/dir or as you specified in OPTIONS).

  encoding  => the encoding used when writing .csv files
 }

INPUT is the input file path; it may be a separate first argument as 
shown above, or else included in OPTIONS as C<< inpath =E<gt> INPUT >>.

If C<outpath =E<gt> OUTPATH> is specifed then results are I<always> saved 
to that path.  With C<allsheets> this is a directory, which will be created
if necessary.

If C<outpath> is NOT specified in OPTIONS then, with one exception,
results are saved to a temporary file or directory and the path returned
as C<outpath> in the result hash.
The exception is if no conversion is necessary 
(i.e. C<cvt_from> is the same as C<cvt_to>), when the
input file itself is returned as C<outpath>.

In all cases C<outpath> in the result hash points to the results.

C<cvt_from> and C<cvt_to> are the filename suffixes (sans dot) of the
corresponding file types, e.g. "csv", "xls", "xlsx", "odt" etc.
These need not be specified when they can be inferred from INPUT
or C<outpath> respectively.

Some vestigial support for spreadsheet formats exists but does not work well
and is not documented here.

OPTIONS may also include:

=over 8

=item sheetname => "sheet name"

The workbook 'sheet' name used when reading or writing a spreadsheet.

An input sheet name may also be specified as "!sheetname" appended to 
the INPUT path.

=item allsheets => BOOL

Valid only with C<< cvt_to =E<gt> 'csv' >>.   
All sheets in the input spreadsheet
are converted to separate .csv files named "SHEETNAME.csv" in
the 'outpath' directory.

=item input_encoding => ENCODING

Specifies the encoding of INPUT if it is a csv file.

ENCODING may be a comma-separated list of encoding
names which will be tried in the order until one seems to work
(requires pre-reading the input file).
If only one encoding is specified it will be used without trying it first.
The default is "UTF-8,UTF-16BE,UTF-16LE,windows-1252".

=item output_encoding => ENCODING

Specifies the encoding to use when writing csv file(s).  
The default it 'UTF-8'.

=item verbose => BOOL

=item use_gnumeric => BOOL   # instead of libre/openoffice (DEPRECATED)

=back

=head3 B<'binmode' Argument For Reading result CSVs>

It is not possible to control the line-ending style in output CSV files,
but the following incantation will correctly read either DOS/Windows (CR,LF) 
or *nix (LF) line endings as a single \n:

   open my $fh, "<", $resulthash->{outpath};
   my $enc = $resulthash->{encoding};
   binmode($fh, ":crlf:encoding($enc)");


=head2 @sane_CSV_read_options

=head2 @sane_CSV_write_options

These contain options you will always want to use with 
S<<< C<< Text::CSV->new() >> >>>.
Specifically, quotes and embedded newlines are handled correctly.

Not exported by default.

=head2 cx2let COLUMNINDEX

=head2 let2cx LETTERCODE

Functions which translate between spreadsheet-column
letter codes ("A", "B", etc.) and 0-based column indicies.
Not exported by default.

=head2 filepath_from_spec EXPR

=head2 sheetname_from_spec EXPR

Functions which decompose strings containing a spreadsheet path and possibly sheetname
suffix, of the form "FILEPATH!SHEETNAME", "FILEPATH|||SHEETNAME", or "FILEPATH[SHEETNAME]".
C<sheetname_from_spec> returns C<undef> if the input does not have a
a sheetname suffix.
Not exported by default.

=head2 form_spec_with_sheetname(PATH, SHEENAME)

Composes a combined string in a "preferred" format (currently "PATH!SHEETNAME").
Not exported by default.

=cut


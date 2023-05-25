# License: http://creativecommons.org/publicdomain/zero/1.0/
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
use File::Temp qw(tempfile tempdir);
use File::Path qw(make_path remove_tree);
use File::Copy ();

use Path::Tiny qw/path/;

# Path::Tiny OBVIATES NEED FOR all but these...
use File::Spec::Functions qw(catdir catfile tmpdir devnull abs2rel);

# Still sometimes convenient...
use File::Basename qw(basename dirname);

use File::Which qw/which/;
use Scalar::Util qw(openhandle blessed);
use Guard qw(guard scope_guard);
use Fcntl qw(:flock :seek);
use Encode qw(decode);
# DDI 5.015 is needed for 'qshlist'
use Data::Dumper::Interp qw/vis visq dvis ivis qsh qshlist u/;
use File::Glob ':bsd_glob';

use Spreadsheet::Edit::Log qw/log_call fmt_call log_methcall fmt_methcall/;
our %SpreadsheetEdit_Log_Options = (
  is_public_api => sub{
    # hide calls
     $_[1][3] =~ /(?:::|^)[a-z][^:]*$/
  },
);

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

sub _is_seekable($) { my $fh = shift; seek($fh, tell($fh), SEEK_SET) }

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
=for Pod::Coverage cxrx2sheetaddr oops btw
=cut
sub cxrx2sheetaddr($$) { # (1,99) -> "B100"
  my ($cx, $rx) = @_;
  return cx2let($cx) . ($rx + 1);
}

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

my $progname = path($0)->basename;
my $lockfile_path = path(File::Spec->tmpdir)->child(__PACKAGE__.".LOCKFILE")->stringify;

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

# Find LibreOffice, or failing that OpenOffice
sub _openlibre_path() {
  state $answer = $ENV{SPREADSHEET_EDIT_LOPATH};
  return $answer if $answer;
  foreach my $short_name (qw(libreoffice loffice localc)) {
    if ($answer = which($short_name)) { return $answer }
  }
  # Search for "isolated" LibreOffice (or OO) installations, which are
  # the result of unpacking from a .deb or other archive into a non-standard 
  # location.  The resulting structure is
  #    <somedir>/opt/{libreoffice,openoffice}*/program/soffice

  # I tried File::Glob::bsd_glob('/*/*/*/opt/libre*/program') but it failed for
  # unknown reasons even though the same glob works from the shell.  
  # Hence this:
  my sub _cmp_subpaths($$) {
    my ($sp1, $sp2) = @_;
    oops     if !defined($sp1);
    return 1 if !defined($sp2);
    # Use longest version in the (sub-)path, e.g. "4.4.1/opt/openoffice4/..."
    my (@v1) = sort { length($a) <=> length($b) } ($sp1 =~ /(\d[.\d]*)/g);
    my (@v2) = sort { length($a) <=> length($b) } ($sp2 =~ /(\d[.\d]*)/g);
    ($v1[-1]//0) <=> ($v2[-1]//0)
  }
  my ($lo_path, $lo_subpath);
  my ($oo_path, $oo_subpath);
  File::Find::find(
    { wanted => sub{
        # Undef fullname OR invalid "_" filehandle implies a broken symlink, 
        #   see https://github.com/Perl/perl5/issues/21122
        # Zero size implies /proc or something else we should not enter.
        # File::Find::fullname unreadable implies followed link to inaccessable 
        #   (The initial "_" stat is invalid, so we can't check -l _)
        $! = 0;
        if (
            !defined($File::Find::fullname) # broken link, per docs
            || (! -r _) || (! -x _) # unreadable item or invalid "_" handle
                                 # https://github.com/Perl/perl5/issues/21122
            || (stat(_))[7] == 0 # zero size ==> /proc or something; ignore
            || ! -r $File::Find::fullname # presumably a symlink to unreadable
            || ! -x _                     # or unsearchable dir
           ) {
          $File::Find::prune = 1;
          return 
        }
        return unless -d _;
        # Look for   /*/<unpackrootdir>/opt/libreofficeXXX/program/
        #             1        2         3     4             5
        #   or     /*/*/<unpackrootdir>/opt/libreofficeXXX/program/
        #           1 2        3         4     5             6
        my $depth = scalar(() = m#(/)#g);
        if ($depth > 4) {
          $File::Find::prune = 1; 
          return;
        }
        if (basename($_) eq "opt") {
          my $subpath = path($_)->parent->realpath->stringify;
          my $path;
          if ($path = bsd_glob "$_/libre*/program/soffice") {
            if (_cmp_subpaths($subpath, $lo_subpath) >= 0) {
              ($lo_path, $lo_subpath) = ($path, $subpath);
            }
          }
          elsif ($path = bsd_glob "$_/open*/program/soffice") {
            if (_cmp_subpaths($subpath, $oo_subpath) >= 0) {
              ($oo_path, $oo_subpath) = ($path, $subpath);
            }
          }
        }
      }, 
      follow_fast => 1,
      follow_skip => 2,
      dangling_symlinks => 0,
      no_chdir => 1
    },
    "/", (defined($ENV{HOME}) ? $ENV{HOME} : ())
  );
  return (($answer = $lo_path)) if $lo_path;
  return (($answer = $oo_path)) if $oo_path;
  croak "Can not find LibreOffice or OpenOffice";
}#_openlibre_path

sub _runcmd($@) {
  my ($opts, @cmd) = @_;
  my $guard = _signals_guard;
  warn "> ",join(" ", map{qsh} @cmd),"\n" if $opts->{verbose};
  my $pid = fork;
  if ($pid == 0) { # CHILD
    ##if ($opts->{stdout_to_stderr}) {
    ##  open(STDOUT, ">&STDERR") or croak $!;
    ##}
    if ($opts->{suppress_stderr}) {
      open(STDERR, ">", devnull()) or croak $!;
    }
    if ($opts->{suppress_stdout}) {
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

# If the arg is an existing directory, return "path/..." else just "path"
sub _fmt_dirish($) {
  my $path = shift;
  -d $path ? catfile($path,"...") : $path
}

# Create a temporary sub-dir of {tempdir} preferably with the given basename,
# but if that name already exists then use a randomized name; this might
# happen when {tempdir} is re-used.
sub _mk_tempsubdir($$) { # does mkdir and returns path
  my ($opts, $namehint) = @_;
  my $basename = path($namehint || oops)->basename;
  my $path = catdir($opts->{tempdir}, $basename);
  if (mkdir $path) {
    return $path;
  }
  my ($base,$suf) = ($basename =~ /^(.*?)((?:\.\w+)?)$/);
  $path = File::Temp::tempfile(DIR => $opts->{tempdir},
                               TEMPLATE => $base."_XXXXX", 
                               SUFFIX => $suf, CLEANUP => 0)
    or oops $!;
  oops($path) unless $path =~ /^\Q$opts->{tempdir}\E\b/;

  btw ">> mkdir $path (namehint=$namehint)" if $opts->{debug};
  $path
}
sub _temp_filename ($$) { # returns a *momentarily* nonexistent file path
  my ($opts, $namehint) = @_;
  my $basename = path($namehint || oops)->basename;
  my $path = catdir($opts->{tempdir}, $basename);
  if (! -e $path) {
    return $path;
  }
  my ($base,$suf) = ($basename =~ /^(.*?)((?:\.\w+)?)$/);
  (undef,$path) = File::Temp::tempfile(DIR => $opts->{tempdir},
                                       TEMPLATE => $base."_XXXXX", 
                                       SUFFIX => $suf, UNLINK => 0);
  oops($path) unless $path =~ /^\Q$opts->{tempdir}\E\b/;
  btw ">> temp_filename $path (namehint=$namehint)" if $opts->{debug};
  $path
}

sub _slurp_binary_file($) {
  my ($input) = @_;
  #return io($input)->all;
  open my $fh, (openhandle($input) ? "<&" : "<"), $input
    or croak "$input : $!\n";
  binmode $fh;  # WARNING: affects arg if *filehandle was passed
  local $/ = undef;
  my $octets = <$fh>;
  unless (openhandle($input)) { 
    close $fh or die $!;
  }
  return $octets;
}

sub _write_binary_tempfile($$) {
  my ($octets, $opts) = @_;
  # Write to a non-auto-deleted file in {tempdir} based on the {inpath} name.
  my $path = path(_mk_tempsubdir($opts, $opts->{inpath}));
WRONG need filename not subdir...
  $path->spew($octets);
  $path->stringify;
}

sub _convert_using_openlibre($) {
  my $opts = shift;
  foreach (qw/inpath cvt_from cvt_to outpath/)
    { oops "missing opts->{$_}" unless exists $opts->{$_} }

  my $outsuf;
  my $lo_cvtto; # "output_file_extension:output_filter_spec"
  if ($opts->{cvt_to} =~ /^([a-z]+):/) {
    $outsuf = $1;
    $lo_cvtto = $opts->{cvt_to};
  } else {
    # N.B. 'man unoconv' has some documentation of import & export filters
    $outsuf = $opts->{cvt_to};
    state $suf2ofilter = {
      csv  => "Text - txt - csv (StarCalc)",
      txt  => "Text - txt - csv (StarCalc)",
      xls  => "MS Excel 97",
      xlsx => "Calc MS Excel 2007 XML",
    };
    my $ofilter = $suf2ofilter->{$outsuf}
      // croak "I don't know how to convert to '$opts->{cvt_to}'\n";
    $lo_cvtto = $outsuf . ":" . $ofilter;
  }

  # I think (not certain) that we can only specify the encoding for CSV files,
  # either as input or output;  .xlsx and .ods spreadsheets (which are based
  # on XML) could in principle use any encoding internally; but not sure
  # we can control that, and anyway at this point UTF8 will be preferred.   
  # Possibly older .xls files will not be handled portably unless encoding 
  # is 'windows-1252' ("WinLatin 1") but ignoring that.  This might be a bug.
  my $encoding = _get_encodings_from_opts($opts) // 'UTF-8';

  my $charset = _name2LOcharsetnum($encoding); # dies if unknown enc

  # New code here attempts to use CSV filter options to *create* a spreadsheet, 
  # e.g. to specify INPUT encoding and, on CSV import and column formats.
  # http://wiki.openoffice.org/wiki/Documentation/DevGuide/Spreadsheets/Filter_Options
  #CSV INPUT FORMAT CONTROL UNTESTED as of 2/6/2021
  if (! $opts->{infilter_opts}) {
    if ($opts->{cvt_from} eq "csv") {
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
      $opts->{infilter_opts} = ":"
        # Tokens 1-5: FldSep=, TxtDelim='"' Charset FirstLineNum CellFormats
        ."44,34,$charset,1,$colformats"
        # Tokens 6-7: LanguageId QuoteAllTextCells
        .",true" ;
    }
  }
  if (! $opts->{outfilter_opts}) {
    if ($opts->{cvt_to} eq "csv") {
      # https://wiki.documentfoundation.org/Documentation/DevGuide/Spreadsheet_Documents#Filter_Options_for_the_CSV_Filter
      $opts->{outfilter_opts} = ":"
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
        .",true"
        # Token 10: "Export cell formulas"
        .",false"
        # Token 11: not used during export
        .","
        # Token 12: (LO 7.2+) sheet selections:
        #   0 or absent => the "first" sheet
        #   1-N => the Nth sheet (arrgh, can not specify name!!)
        #   -1 => export all sheets to files named filebasenamne.Sheetname.csv
        .",".(
           $opts->{allsheets} ? 
             _openlibre_supports_allsheets() ? -1 
             : croak("Your Libre Office version does not support 'allsheets'")
           :
           defined($opts->{sheetname}) ? (
             (_openlibre_supports_named_sheet() || $opts->{sheetname} =~ /^[1-9]\d*$/) ? $opts->{sheetname}
             :
             croak("Libre Office does not support specifying a sheet by name")
           )
           : ""
       )
       ;
    }  
  }

  # LibreOffice (and unoconv) sometimes aborts if the same user has LO open
  # interactively.  Forcing a separate user-config dir seems to avoid this.
  # https://ask.libreoffice.org/en/question/290306/how-to-start-independent-lo-instance-process
  my $saved_UserInstallation = $ENV{UserInstallation};
  { my $EUID = $>;
    # Unique per user.  We use a lockfile to prevent concurrent access,
    # so sharing the profile among all processes is okay. 
    # Measured as 20% faster when re-using an existing profile.
    my $profile_dir = path(File::Spec->tmpdir)
                        ->child(__PACKAGE__."_${EUID}_LOprofile");
    mkdir $profile_dir;
    if (! -e $profile_dir) {
      croak "$profile_dir : $!";
    } else {
      croak "$profile_dir is not a directory owned by you!\n"
        unless (-d _ && -o _);
      $ENV{UserInstallation} = "file://$profile_dir";
    }
  } 
  scope_guard {
    if (defined $saved_UserInstallation) {
      $ENV{UserInstallation} = $saved_UserInstallation;
    } else {
      delete $ENV{UserInstallation}
    }
  };

  # We can only control the output directory path, not the name of
  # the specific files.  If {outpath} is a directory then the result
  # can be put there directly, detectable afterwards as being not 
  # in $opts{existing_in_outpath}.
  #
  # Otherwise we create a temp dir and later move the output file
  # to the desired destination.
  my $_outdir_is_temp;
  if (defined($opts->{existing_in_outpath}) || -d $opts->{outpath}) {
    $opts->{_outdir} = $opts->{outpath};
  } else {
    $opts->{_outdir} = catfile($opts->{tempdir}, "LO-OUTDIR");
    $_outdir_is_temp = 1;
  }

  # Note: unoconv seems unsupported and now spews warnings about an old
  # Python library, so we no longer use it.  Old code is in commit cac7a76b
  
  my $prog = _openlibre_path() // croak "Libre/Open Office not found";
  my @cmd = ($prog, "--headless", "--invisible",
                    "--convert-to", $lo_cvtto.($opts->{outfilter_opts}//""),
                    "--outdir", $opts->{_outdir},
                    $opts->{inpath_sans_sheet});

  $opts->{suppress_stdout} = !$opts->{debug}; # avoid "convert ..." message
  
  # Arrgh! As of LO 7.5, we get zero exit status even if the input
  # files do not exist or are not readable.
  # https://bugs.documentfoundation.org/show_bug.cgi?id=155415
  unless (-r $opts->{inpath_sans_sheet}) {
    croak $opts->{inpath_sans_sheet}." is missing or unreadable\n";
  }
  if (-e $opts->{_outdir} && ! -w _) {
    croak $opts->{_outdir}." exists but is not writable\n";
  }

  my $cmdstatus = _runcmd($opts, @cmd);

  if ($cmdstatus != 0) {
    if ("@cmd" =~ / -o \/tmp\/out\./ || ($ENV{PWD}//"") eq "/tmp" && "@cmd" =~ / -o (?:\.\/)?out\./) {
      btw "**KNOWN unoconv bug causes abort if output file is /tmp/out.* (yes, strange)\n";
    }
    croak "($$) Conversion of '$opts->{inpath}' to $outsuf failed\n",
          "(make sure libre/open office is not running)\n"
  }

  if ($opts->{allsheets}) {
    # Rename files to match our API (omit the spreadsheetbasename- prefix)
    oops if $_outdir_is_temp;
    my $outdir = path $opts->{_outdir};
    foreach my $orig (map{$_->basename} $outdir->children) {
      next if $opts->{existing_in_outpath}->{$orig};
      (my $new = $orig) =~ s/^$opts->{basename}-// or oops dvis '$orig $opts->{basename}';
      my $orig_path = $outdir->child($orig);
      my $new_path  = $outdir->child($new);
      btw ">> Renaming $orig_path -> $new\n" if $opts->{debug};
      File::Copy::move($orig_path, $new_path) or oops "$!";
    }
  } else {
    # Move the output file to the desired destination
    oops unless $_outdir_is_temp;
    my $thefile = path($opts->{_outdir})->child("$opts->{basename}.$opts->{cvt_to}");
    unless ($thefile->exists) {
      system "set -x; ls -la ".qsh($opts->{_outdir})." >&2"; 
      oops "Expected file not found: ",qsh($thefile);
    }
    File::Copy::move($thefile, $opts->{outpath})
      or die "move to ", _fmt_dirish($opts->{outpath})," failed: $!";
    btw ">> Removing temp dir ",qsh($opts->{_outdir}),"\n" if $opts->{debug};
    path($opts->{_outdir})->remove_tree;
    remove_tree($opts->{_outdir});
  }

  return ($encoding);
}

sub _convert_using_gnumeric($) {  # use ssconvert
  my $opts = shift;
  confess "Deprecated with extreme prejudice"; # no longer supported

  foreach (qw/inpath cvt_to outpath/)
    { oops "missing opts->{$_}" unless exists $opts->{$_} }

  my $eff_outpath = $opts->{outpath};
  if (my $prog=which("ssconvert")) {
    my $enc = _get_encodings_from_opts($opts);
    $enc //= "UTF-8"; # default
    my @options;
    if ($opts->{cvt_to} eq "csv") {
      push @options, '--export-type=Gnumeric_stf:stf_assistant';
      my @dashO_terms = ("format=preserve", "transliterate-mode=escape");
      push @dashO_terms, "charset='${enc}'" if defined($enc);
      if ($opts->{sheetname}) {
        push @dashO_terms, "sheet='$opts->{sheetname}'";
      }
      if ($opts->{allsheets}) {
        #If both {allsheets} and {sheetname} are specified, only a single
        # .csv file will be in the output directory
        croak "'allsheets' option: 'outpath' must specify an existing directory"
          unless -d $eff_outpath;
        $eff_outpath = catfile($eff_outpath, "%s.csv");
        push @options, "--export-file-per-sheet";
      }
      elsif ($opts->{sheetname}) {
        # handled above
      }
      else {
        # A backwards-incompatible change to ssconvert stopped extracting
        # the "current" sheet by default; now all sheets are concatenated!
        # See https://gitlab.gnome.org/GNOME/gnumeric/issues/461
        # ssconvert verison 1.12.45 supports a new "-O active-sheet=y" option
  ## PORTABILITY BUG: Redirection syntax will not work on windows
        my ($ssver) = (qx/ssconvert --version 2>&1/ =~ /ssconvert version '?(\d[\d\.]*)/);
        if (version::is_lax($ssver) && version->parse($ssver) >= v1.12.45) {
          push @dashO_terms, "active-sheet=y";
        } else {
          croak("Due to an ssconvert bug, a sheetname must be given.\n",
                "(for more information, see comment at ",__FILE__,
                " near line ", (__LINE__-10), ")\n");
        }
      }
      push @options, '-O', join(" ",@dashO_terms);
    }
    elsif ($opts->{cvt_to} eq 'xlsx') {
      @options = ('--export-type=Gnumeric_Excel:xlsx2');
    }
    elsif ($opts->{cvt_to} eq 'xls') {
      @options = ('--export-type=Gnumeric_Excel:excel_biff8'); # M'soft Excel 97/2000/XP
    }
    elsif ($opts->{cvt_to} =~ /^od/) {
      @options = ('--export-type=Gnumeric_OpenCalc:odf');
    }
    elsif ($eff_outpath =~ /\.[a-z]{3,4}$/) {
      # let ssconvert choose based on the output file suffix
    }
    else {
      croak "unrecognized cvt_to='".u($opts->{cvt_to})."' and no outpath suffix";
    }

    my $eff_inpath = $opts->{inpath};
    if ($opts->{sheetname} && $opts->{inpath} =~ /.csv$/i) {
      # Control generated sheet name by using a symlink to the input file
      # See http://stackoverflow.com/questions/22550050/how-to-convert-csv-to-xls-with-ssconvert
      my $td = catdir($opts->{tempdir} // oops, "Gnumeric");
      remove_tree($td); mkdir($td) or die $!;
      $eff_inpath = catfile($td, $opts->{sheetname});
      symlink $opts->{inpath}, $eff_inpath or die $!;
    }
    my @cmd = ($prog, @options, $eff_inpath, $eff_outpath);

    my $suppress_stderr = !$opts->{debug};
    if (0 != _runcmd({%$opts, suppress_stderr => $suppress_stderr}, @cmd)) {
      # Before showing a complicated ssconvert failure with backtrace,
      # check to see if the problem is just a non-existent input file
      { open my $dummy_fh, "<", $eff_inpath or croak "$eff_inpath : $!"; }
      my $failmsg = "($$) Conversion of '$opts->{inpath}' to $eff_outpath failed\n"."cmd: ".qshlist(@cmd)."\n";
      if ($suppress_stderr) {  # repeat showing all output
        if (0 == _runcmd({%$opts, suppress_stderr => 0}, @cmd)) {
          warn "Surprise!  Command failed the first time but succeeded on 2nd try!\n";
        }
        croak $failmsg;
      }
    }
    elsif (! -e $opts->{outpath}) {
      croak "($$) Conversion SILENTLY failed\n(using $prog)\n",
            "  cmd: ",qshlist(@cmd),"\n"
            ;
    }
    return ($enc)
  }
  else {
    croak "Can not find ssconvert to convert '$opts->{inpath}' to $opts->{cvt_to}\n",
        "To install ssconvert: sudo apt-get install gnumeric\n";
  }
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

# Convert between spreadsheet and CSV file (either direction),
#   handling pre-opened input, non-seekable input, and no-op conversions
#
# INPUT ARGUMENTS: inpath, optname => value ...
#   inpath                         A pathname string or pre-opened filehandle;
#                                    If not seekable, data is copied to a
#                                    temp file (auto-removed at process exit).
#
#   cvt_from => "csv"|"xls"|...    Input file format;
#                                    Auto-detected if missing or empty.
#
#   cvt_to => "csv"|...            Output file format; required unless
#                                    <outpath> is specified with a
#                                    recognized .suffix
#
#   sheetname => "sheetname"       Specifies which sheet if inpath is a
#                                    spreadsheet file; may also be given
#                                    as a suffix within <inpath> using any of
#                                    several forms (for historical reasons):
#                                    !sheetname or [sheetname] or |||sheetname
#
#   allsheets => BOOLEAN           If true, every sheet in a spreadsheet is
#                                    converted to a corresponding csv in an
#                                    output *directory* [valid only with
#                                    cvt_to => "csv"]
#
#   iolayers => "..."              Perl open() I/O layers specifier for
#                                    reading/writing CSV files e.g.
#                                    ":perlio:encoding(utf8)"; auto-detected
#                                    if necessary for input CSVs.
#
#   outpath => "..."               Usually omitted. If specified, the results
#                                    are written to the specified path.
#                                    If NOT specified, results are written to
#                                    a temporary file which is auto-removed at
#                                    process exit, or if no conversion is
#                                    needed (and the input is seekable) then
#                                    the original input is returned.
#                                    May *not* be specified if conversion
#                                    might be unnecessary.
#
#                                    With allsheets => true, outpath is a
#                                    *directory* in which a csv file for each
#                                    sheet will be created.
#   tempdir => "/tmp" or whatever
#   verbose => bool
#   debug => bool
#
# RETURNS:
#   {
#     cvt_from  => as specified or defaulted/auto-detected
#     cvt_to    => ditto
#     iolayers  =>
#     inpath    => as spec. or a tempfile copy if input isn't seekable
#     sheetname => actual sheet name read or created in spreadsheet
#     outpath   => where the result is; if <outpath> was specified
#                  in the args then result is always written there,
#                  otherwise could be a temp file (auto-removed
#                  at process exit) or the same as <inpath> if no
#                  conversion was needed.
#     verbose & debug => as specified
#   }
sub _process_args($;@) { # returns (key => value, ...)
  confess "fix obsolete call to pass linearized options" if ref($_[0]) && !blessed($_[0]);
  my $separate_inpath;
  if (scalar(@_) % 2) { # odd number of args
    # Treat an initial or singular arg as inpath
    # TODO: Consider eliminating this API and require only key => value pairs
    #   THAT WOULD BE AN INCOMPATIBLE API RELEASE!
    $separate_inpath = shift;
  }
  my %opts = (
              iolayers => "",
              cvt_from => "",
              cvt_to => "",
              ##stdout_to_stderr => 1,  # see &_runcmd()
              @_,
              #verbose => 999, tempdir => "/tmp/J",
            );
  if (defined $opts{inpath}) {
    croak "Initial INPATH arg specified as well as inpath => ... in options"
      if defined $separate_inpath;
  } else {
    $opts{inpath} = $separate_inpath;
  }
  $opts{inpath} //= $separate_inpath; 
  $opts{verbose}=1 if $opts{debug};
  if (exists $opts{encoding}) {
    Carp::cluck "Using OBSOLETE csv 'encoding' opt (use iolayers => \":encoding(...)\" instead)\n";
    $opts{iolayers} .= ":encoding(". delete($opts{encoding}) .")";
  }
  # inpath or outpath may have "!sheetname" appended (or alternate syntaxes),
  # but may exist only if a separate 'sheetname' option is not specified.
  # Input and output can not both be spreadsheets; one must be a CSV.
  if (exists($opts{sheet})) {
    carp "WARNING: Deprecated 'sheet' option found (use 'sheetname' instead)\n";
    croak "Both {sheet} and {sheetname} specified" if exists $opts{sheetname};
    $opts{sheetname} = delete $opts{sheet};
  }
  { my ($sheet_from_path, $key_with_sn);
    for my $key ('inpath', 'outpath') {
      my $spec = $opts{$key} || next;
      next if openhandle($spec);
      # Split filepath!sheetname  etc.
      if (my $sn = sheetname_from_spec($spec)) {
        croak "Both $key_with_sn and $key specify a sheetname embedded in path"
          if $key_with_sn;
        ($sheet_from_path, $key_with_sn) = ($sn, $key);
      }
    }
    if ($opts{sheetname}) {
      croak "{sheetname} option conflicts with sheetname embedded in path\n",
            "   opt sheetname => ", qsh($opts{sheetname}),"\n",
u           "   $key_with_sn is ", qsh($opts{$key_with_sn}),"\n"
        if defined($sheet_from_path) && $sheet_from_path ne $opts{sheetname};
    }
    elsif (defined $sheet_from_path) {
      btw "(extracted sheet name \"$sheet_from_path\" from $key_with_sn)\n"
        if $opts{verbose};
      $opts{sheetname} = $sheet_from_path;
    }
  }
  unless (openhandle($opts{inpath})) {
    $opts{inpath_sans_sheet} = filepath_from_spec $opts{inpath};
  }
  %opts
}
sub _detect_to_from($) { # updates %$opts and returns the effective inpath
  my $opts = shift;
  unless ($opts->{cvt_to}) {
    if ($opts->{outpath} && $opts->{outpath} =~ /\.([&.]+)$/) {
      $opts->{cvt_to} = $1;
    }
    croak "cvt_to was not specified and can not be intuited from outpath"
      ,dvis('\n### $opts')  ###TEMP
      unless $opts->{cvt_to};
  }
  unless ($opts->{cvt_from}) {
    if ($opts->{inpath_sans_sheet} && $opts->{inpath_sans_sheet} =~ /\.([&.]+)$/) {
      $opts->{cvt_from} = $1;
      $opts->{cvt_from} =~ s/^\.txt$/.csv/i;
    }
  }
  my $eff_inpath = $opts->{inpath_sans_sheet};
  if (!$opts->{cvt_from}
      ||
      $opts->{cvt_from} eq "csv" && !defined( _get_encodings_from_opts($opts) )
     )
  {
    # Peek at the data to auto-detect a CSV file, or if known to be CSV
    # then auto-detect the encoding if the encoding was not specified.
    my $fh;
    if (openhandle($opts->{inpath})) {
      open $fh, "<&", $opts->{inpath} or croak "dup $opts->{inpath} : $!\n";
    } else {
      open $fh, "<", $opts->{inpath_sans_sheet} 
        or croak "$opts->{inpath_sans_sheet} : $!";
    }
    my $octets = _slurp_binary_file($fh);
    my $empty = length($octets)==0;

    if (! _is_seekable($fh) or $empty) { ### WHY or $empty ???
      # Make a copy if we won't be able to re-read the input later 
      $eff_inpath = _write_binary_tempfile($octets, $opts);
    }

    if ($opts->{cvt_from}) {
      if ($opts->{cvt_from} eq "csv") {
        _update_iolayers($octets, $opts); # auto-detect or verify encoding
      }
    } else {
      # Auto-detect CSV by looking for comma-separated fields; but first
      # try to decode characters, which may fail if it is not really a CSV.
      eval { local $opts->{debug}=0;           # don't need to see decode errs
             _update_iolayers($octets, $opts); # auto-detect encoding
           };
      if (! $@) {
        # No decode errors occurred in check above, so assume it is a text file
        # N.B. _update_iolayers set $opts->{iolayers} with encoding.
        my $enc = _get_encodings_from_opts($opts) // croak dvis '%$opts';
        my $chars = decode($enc, $octets, Encode::FB_CROAK|Encode::LEAVE_SRC);
        # Does the data look like a csv file?
        my $min_cols_minus1 = 3 - 1;
        if ($chars =~ /\A(?:.*?,){$min_cols_minus1,}(.*?)[\x{0A}\x{0D}]/s
             or $empty) {
          warn "Presuming \"$opts->{inpath}\" contains CSV data\n"
            if $opts->{verbose};
          $opts->{cvt_from} = 'csv';
        }
        # else: It seems to be a text file but is not a CSV !
      }
    }
    if (!$opts->{cvt_from}) {
      # It must be some kind of spreadsheet.
      if (openhandle($eff_inpath)) {
        seek $eff_inpath, 0, SEEK_SET or die "seek $eff_inpath : $!"
      }
    }
  }
  return $eff_inpath;
}#_detect_to_from

sub _openlibre_features() {
  state $hash;
  return $hash if defined $hash;
  my $prog = _openlibre_path() // croak "Libre/Open Office not found";
  my ($s) = (qx/$prog --version/ =~ /Libre.*? (\d+\.[\d\.]*)/);
  my $version = version->parse("v".($s//"0.1"));
  $hash = {
    # LibreOffice 7.2 allows extracting all sheets at once
    allsheets => ($version >= version->parse("v7.2")),
    # ...but not yet extracting a single sheet by name.
    # https://bugs.documentfoundation.org/show_bug.cgi?id=135762#c24
    named_sheet => 0,
  }
}
sub _openlibre_supports_allsheets() { _openlibre_features()->{allsheets} }
sub _openlibre_supports_named_sheet() { _openlibre_features()->{named_sheet} }

sub convert_spreadsheet(@); # forward
sub _emulate_by_sheet_name($) {
  my %opts = %{ $_[0] };
  # Emulate extracting a single sheet by name by extracting all sheets
  # and discarding all but the one we want
  oops "recursion?" if u($opts{outpath}) =~ /EMULATE/;
  warn ">>Emulating extract-by-name by extracting all...\n" 
    if $opts{verbose};
  #btw dvis('%opts\n') if $opts{debug};
  my %topts = %opts;
  delete @topts{qw/sheetname inpath_sans_sheet existing_in_outpath/};
  $topts{inpath} = $opts{inpath_sans_sheet} 
    unless openhandle($opts{inpath});
  $topts{allsheets} = 1;
  $topts{outpath} = _mk_tempsubdir(\%opts, "_EMULATE-BY-NAME");
  unless ($opts{debug}) {
    $topts{silent} = 1;
    $topts{verbose} = $topts{debug} = 0;
  }
  my $hash = convert_spreadsheet(%topts) // oops;

  my @matches = grep{ /$opts{sheetname}\.csv$/ } 
                map{$_->basename} path($topts{outpath})->children;
  oops dvis '@matches\n$topts' unless @matches==1;
  my $thepath = catfile($topts{outpath}, $matches[0]);
  $opts{outpath} //= catfile($opts{tempdir}, $matches[0]);
  btw ">> move ",qsh($thepath)," -> ",qsh(_fmt_dirish $opts{outpath}),"\n"
    if $opts{debug};
  File::Copy::move( $thepath, $opts{outpath} )
    or die "move of $thepath failed ($!)";
  btw ">> Removing intermediate dir ",qsh($topts{outpath}),"\n" 
    if $opts{debug};
  remove_tree($topts{outpath}) or die "$topts{outpath} : $!";
  $hash->{sheetname} = $opts{sheetname};
  $hash->{outpath}   = $opts{outpath};
  return $hash
}

sub convert_spreadsheet(@) {
  my %opts = &_process_args;
  my %input_opts = %opts;

  btw dvis('>>> convert_spreadsheet %input_opts\n') if $opts{debug};
  
  my sub _fmt_outpath_content($) {
    my $outpath = $_[0]->{outpath} // oops;
    return "" unless -d $outpath;
    "\n  outpath contains: "
           .join(", ",map{qsh basename $_} path($outpath)->children);
  }

  # FIXME sorta-BUG HERE:
  #   Should re-use the same tempdir (?) to avoid proliferation of tempdirs
  #   if many spreadsheets are read by the same process?  We can only remove
  #   a {tempdir} at process exit because it may contain a result file
  #   returned in {outpath}.
  #
  #   If we re-use the same {tempdir} then must ensure unique generated names!
  #
  # RELATED QUESTION:
  #   Should we return a self-deleting Path::Tiny::tempfile object in outpath?
  #
  #$opts{tempdir} //= File::Temp::tempdir("/tmp/spread_XXXXXX", CLEANUP=>1);
  
  # Re-use the same tempdir for the duration of this process, to avoid
  # proliferation when multiple calls are made.  {tempdir} is removed at
  # process exit unless the user specified {tempdir}.
  #
  # THREAD SAFETY: {outpath} should ALWAYS be specified when not using
  #   the 'allsheets' option, becuase temporary *files* are not atomically
  #   created and so threads might race (temp *directories* are atomically
  #   created, so the result of `allsheets` should be thread-safe).
  #
  # FIXME: Should we pre-create temp files and then *copy* data into them
  #   if the underlying utility can't over-write an existing file???
  #   Or is it better to create a unique temp subdir for the output file
  #   and return a file in it, or atomically move the file up before rtn or ???
  #
  state $tempdir;
  $opts{tempdir} 
    //= ($tempdir //= File::Temp::tempdir("/tmp/spread_XXXXXX", CLEANUP=>1));
  # NOTE: _mk_tempsubdir() and _temp_filename() handle clashes with existing 

  my $eff_inpath = _detect_to_from(\%opts); # could be an open fh
#btw dvis '# # #AAA $eff_inpath\n%opts';

  my $inpathish = $opts{inpath_sans_sheet} // $opts{inpath}; 
  { my $ifbase = path($inpathish)->basename(qr/\.[^.]+/);
    # If inpathish is a file handle it will stringify like "GLOB(0xabcdef...)"
    $ifbase =~ s/[^-.,;= \w]/_/g; # make it acceptable as part of a filename
    $opts{basename} = $ifbase;
  }
  my $outpath_isdir;
  if ($opts{allsheets}) {
    if ($opts{sheetname}) {
      croak "With 'allsheets', a sheet name may not be specified\n";
    }
    $opts{outpath} //= catfile($opts{tempdir}, $opts{basename});
    if (-e $opts{outpath}) {
      croak "With allsheets, a pre-existing outpath must be a directory" 
        unless -d _;
    } else {
      mkdir $opts{outpath} or croak "mkdir $opts{outpath} : $!";
    }
    warn "> Extracting sheets from $opts{cvt_from} ",qsh($opts{inpath}),
          " into ",qsh($opts{outpath}),"/*.$opts{cvt_to}\n"
      if $opts{verbose};
    $outpath_isdir = 1;
  } else {
    $outpath_isdir = defined($opts{outpath}) && -d $opts{outpath};
    $opts{outpath} //= catfile($opts{tempdir}, 
                 ($opts{sheetname} || $opts{basename}).".".$opts{cvt_to});
    # Will store single result inside directory {outpath} if it pre-exists
    warn "> Converting $opts{cvt_from} ",
          qsh(form_spec_with_sheetname($inpathish, $opts{sheetname})),
          " to $opts{cvt_to} ", qsh(_fmt_dirish $opts{outpath}), "\n"
      if $opts{verbose};
  }
  if ($outpath_isdir) {
    $opts{existing_in_outpath} = {
      map{($_ => 1)} path($opts{outpath})->children
    };
  }

  if ($opts{cvt_from} eq $opts{cvt_to}) {
    # Special case #1: No conversion is needed: Just copy the file or 
    #   return the input path itself (or a seekable temp copy) as the output
    if (!$opts{allsheets}) {
      if (defined $opts{outpath}) {
        warn "  No conversion needed, copying to ", 
              qsh(_fmt_dirish $opts{outpath}),"\n"
          if $opts{verbose};
        File::Copy::copy($eff_inpath, $opts{outpath});
      } else {
        $opts{outpath} = $eff_inpath; # possibly a temp copy
        warn "  No conversion needed, returning ", qsh($opts{outpath}),"\n"
          if $opts{verbose};
      }
    }
    # Special case #2: <allsheets> with input already a csv:
    #   Leave a symlink to the input in the <outpath> directory.
    elsif ($opts{allsheets}) {
      if ($opts{cvt_to} eq "csv") {
        my $linktarget = abs2rel($eff_inpath, $opts{outpath});
        my $linkpath = path($opts{outpath})->child(path($inpathish)->basename);
        symlink($linktarget, $linkpath) or croak "symlink $linkpath : $!";
        warn "  No conversion needed, leaving symlink to input at ", qsh($linkpath),"\n"
          if $opts{verbose};
      } else {
        croak "{allsheets} not supported with cvt_to=",vis($opts{cvt_to});
      }
    }
    else {
      oops dvis '%opts'
    }
  }
  else {
    # 5/12/2023: Formerly we used gnumeric unless specifically told otherwise.
    # Now that we have a fix for concurrency (separate $UserInstallation dirs)
    # and now that LO (v7.2) supports "allsheets" mode we use LO for everything
    # which obviates the need to install gnumeric, which is not supported
    # on non-*ix platforms.  Currently the gnumeric code is stale...
    croak "'use_gnumeric' is no longer supported\n" if $opts{use_gnumeric};

    $opts{use_gnumeric} //=
            ( $opts{allsheets} && !_openlibre_supports_allsheets() )
         #We emulate named_sheet with allsheets...
         #|| ( $opts{sheetname} && !_openlibre_supports_named_sheet() );
         || ( $opts{sheetname} && !_openlibre_supports_allsheets() );
       
    if ($opts{cvt_to} eq "csv" && defined($opts{sheetname}) 
        && !_openlibre_supports_named_sheet()
        && _openlibre_supports_allsheets()
       ) {
      my $hash =_emulate_by_sheet_name(\%opts);
      log_call [\%input_opts,\"(emulated single-sheet extract)"],
               [$hash, \_fmt_outpath_content($hash)] 
        if $opts{verbose};
      return $hash;
    }

    # Prevent concurrent conversions of different documents (e.g. in pipeline)
    # (Open/Libre Office have bugs which prevent this)
    open (my $lock_fh, "+>>", $lockfile_path) or die $!;
    chmod 0666, $lock_fh;
    scope_guard {
      truncate($lock_fh,0);
      flock($lock_fh, LOCK_UN) or die "flock UN: $!";
    };
    if (! flock($lock_fh, LOCK_EX|LOCK_NB)) {
      seek($lock_fh,0,SEEK_SET) or die;
      (my $owner = do{ local $/; <$lock_fh> }) =~ s/\s*\z//s;
      {
        last unless $owner =~ /pid (\d+)/;
        last unless my @s = stat("/proc/$1");
        last unless my @pw = getpwuid($s[4]);
        $owner = $pw[0]." ".$owner; # user name
      }
      warn ">> ($$) Waiting for exclusive lock owned by $owner to convert spreadsheet...\n";
      flock($lock_fh, LOCK_EX) or die "flock: $!";
    }
    print $lock_fh "pid $$ ($progname)\n"; # always appends

    $opts{use_gnumeric} = 0 if $opts{col_formats}; # must use libreoffice

    my $encoding;
    if ($opts{use_gnumeric}) {
      croak "{col_formats} is not supported by gnumeric" if $opts{col_formats};
      ($encoding) = _convert_using_gnumeric(\%opts);
    } else {
      ($encoding) = _convert_using_openlibre(\%opts);
    }
    oops unless -r $opts{outpath};

    $opts{iolayers} = ":encoding($encoding)" if defined($encoding);
  }

  my $retval = {
    map{ ($_ => $opts{$_}) }
    grep{ defined $opts{$_} }
    qw(inpath sheetname outpath iolayers cvt_from cvt_to verbose debug)
  };
  log_call [\%input_opts], [$retval, \_fmt_outpath_content($retval)]
    if $opts{verbose};
  $retval;
}

# Extract encoding(s) from {encoding} or {iolayers}
#   If multiple encodings (to try, in order) are specified,
#   then in scalar context the *first* one is returned.
# Returns () or undef if no encodings are specified
sub _get_encodings_from_opts($) {
  my $opts = shift;
  my ($enclist) = ($opts->{iolayers} =~ /(?:^|:)encoding\(([^()]*)\)/);
  $enclist //= $opts->{encoding};
  unless ($enclist) {
    return wantarray ? () : undef
  }
  if ($opts->{encoding} && $opts->{encoding} ne $enclist) {
    croak "BUG: Incompatible opts{encoding}='$opts->{encoding}' and {iolayers}='$opts->{iolayers}'"
  }
  my @enclist = split /,/, $enclist;
  wantarray ? @enclist : $enclist[0];
}

# $opts may specify none, one, or multiple encoding options; if none are
# specified then a default list is used, so this should always succeed
# by default if the data is UTF-8.
#
# Each encoding is tried on the sample data and the first which works is
# returned.  An exception is thrown if none work.
sub _detect_encoding($$) {
  my ($octets, $opts) = @_; croak "Undef input" unless defined $octets;

  my @enclist = _get_encodings_from_opts($opts);
  @enclist = ("UTF-8","windows-1252") if @enclist==0; # guessed default

  foreach my $enc (@enclist) {
    eval { decode($enc, $octets, Encode::FB_CROAK|Encode::LEAVE_SRC) };
    if ($@) {
       btw "Encoding '$enc' did not work...($@)\n" if $opts->{debug};
       next;
    }
    btw "Encoding '$enc' seems to work.\n" if $opts->{debug};
    return $enc;
  }
  croak "None of the encodings \"",join(",",@enclist),"\" are correct!\ninpath: ",qsh($opts->{inpath}),"\n"
}

sub _update_iolayers($$) {
  # MODIFIES $opts->{iolayers} to specify a single encoding, and crlf option
  my ($octets, $opts) = @_; croak "Undef input" unless defined $octets;
  my $orig = $opts->{iolayers};

  my $encoding = _detect_encoding($octets, $opts); # dies if unrecognizable

  my $lineend_layer;
  # N.B. Final line may be missing newline (e.g. downloads from Google Docs)
  if ($octets =~ /\x0d\x0a/) {
    $lineend_layer = ":crlf";
  }
  elsif ($octets =~ /\x0a/) {
    $lineend_layer = ":perlio";  # Force UNIX line endings, see perldoc PerlIO
  }
  elsif (length($octets) == 0) {
    $lineend_layer = ":perlio";  # Doesn't really matter for an empty file
  }
  else {
    croak u($opts->{inpath})," : Could not detect line ending convention. File ends with: ",
            join( "", map{ sprintf " 0x%02X", ord($_) }
                      split //,substr($octets,-40) ),
            "\n";
  }

  $opts->{iolayers} =~ s/(^|:)( encoding\([^()]*\) | crlf | raw )//gx;
  $opts->{iolayers} =~ s/^/"${lineend_layer}:encoding($encoding)"/e;
  btw "_update_iolayers (l",(caller)[2],") ",
               "'",u($orig),"' -> '$opts->{iolayers}'\n" if $opts->{debug};
}


# Open as a CSV, intuiting input encoding, converting spreadsheet if necessary.
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
  # TODO: Rename {path} to {inpath} in all usages and rm this cruft:
  carp "Obsolete OpenAsCsv usage: Change path to inpath\n"
    if exists($opts{path}) and !$opts{silent};
  $opts{inpath} //= delete $opts{path}; # be compatible with old API 

  # TODO: Consider renaming {inpath} as {input} bc it can be a filehandle
  #   (massive changes required; in some contexts it must be a path...)

  my $inpath = delete $opts{inpath};
  croak "inpath is a ref! (".ref($inpath).")" if ref($inpath) && !blessed($inpath);
  croak "OpenAsCsv: missing 'inpath' option\n" unless $inpath;
  croak "OpenAsCsv: outpath may not be specified\n" if $opts{outpath};

  my $h = convert_spreadsheet($inpath, %opts, verbose => $opts{debug});
  croak "sheetname key bug" if exists $h->{sheet};
  my $csvpath = $h->{outpath}; # may be same as {inpath} if already a CSV
  my $iolayers = $h->{iolayers};

  open my $fh, (openhandle($csvpath) ? "<&" : "<"), $csvpath
    or croak "$csvpath : $!\n";
  binmode $fh, $iolayers or die $!;

  my $r = {
    fh            => $fh,
    csvpath       => $csvpath,
    iolayers      => $iolayers,
    inpath        => $inpath,
    sheetname     => $h->{sheetname},
    sheet         => $h->{sheetname}, # deprecated
  };
  $r->{tempdir} = $opts{tempdir} if $opts{tempdir};

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
  print "Reading ",$hash->csvpath()," with encoding ",$hash->encoding(),"\n";
  while (<$hash->{fh}>) { ... }

  # Convert CSV to spreadsheet
  $hash = convert_spreadsheet(inpath => "mycsv.csv", cvt_to => "xls");
  print "Resulting spreadsheet path is $hash->{outpath}\n";
  
  # Convert a single sheet from a spreadsheet to CSV
  $hash = convert_spreadsheet(inpath => "mywork.xls", sheetname => "Sheet1", 
                              cvt_to => "csv");
  open my $fh, (openhandle($hash->{outpath}) ? "<&" : "<"), $hash->{outpath};
  binmode $fh, $hash->{iolayers};
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

INPUT may be a csv or spreadsheet workbook path or an open filehandle
to one of those; if a spreadsheet, then a single "sheet" is 
converted, specified by either a !SHEETNAME suffix 
in the INPUT path, or a separate C<< sheetname => SHEETNAME >> option.

The resulting file handle refers to a guaranteed-seekable CSV file; 
this will either be a temporary file (auto-removed at process exit), 
or the original INPUT if it was already a seekable csv file.

RETURNS: A ref to a hash containing the following:

 {
  fh        => the resulting open file handle
  iolayers  => the iolayers (i.e. binmode arg) used by the file handle
  csvpath   => the path {fh} refers to, which might be a temporary file
  inpath    => original input path or open file handle
  sheetname => sheet name if the input was a spreadsheet
  tempdir   => temporary directory, only if specified in input arguments
 }

=head2 convert_spreadsheet INPUT, cvt_to=>suffix, OPTIONS

=head2 convert_spreadsheet INPUT, cvt_to=>"csv", allsheets => 1, OPTIONS

This converts from CSV to one of various spreadsheet formats or vice-versa.

RETURNS: A ref to a hash containing at least:

 {
  outpath   => path of output file (or directory with 'allsheets')
  iolayers  => (i.e. binmode arg) needed to read output if CSV
  inpath    => path of original file with any !SHEETNAME suffix removed
  sheetname => sheet name if the input was a spreadsheet
  cvt_from  => input file type, as specified or detected
  cvt_to    => output file type, as specified or derived from outpath
 }

C<cvt_from> and C<cvt_to> are the filename suffixes (sans dot) of the
corresponding file types, e.g. "csv", "xls", "xlsx", "odt" etc.

INPUT may be a path or a pre-opened file handle.  If C<cvt_from> is
is not specified then it is inferred from the INPUT path suffix, 
or if INPUT is a handle or has no suffix then the file content is examined.

If outpath => OUTPATH is specifed in OPTIONS then results are returned there.
OUTPATH must have the appropriate file suffix (.csv .xls etc.) 
except with C<< allsheets => 1 >> when OUTPATH, if specified,
must be a directory path which will be created if it does not exist.

If outpath => OUTPATH is B<not> specified (or is undef) then results are 
returned in temporary file(s) which are auto-deleted at process exit, 
except that if no conversion is
necessary (C<cvt_from> is the same as C<cvt_to>) then INPUT itself is
returned as C<outpath>. 

Some vestigial support for spreadsheet formats exists but does not work well
and is not documented here.

Other OPTIONS may include:

=over 8

=item sheetname => "sheet name"

The workbook 'sheet' name used when reading or writing a spreadsheet.

An input sheet name may also be specified as "!sheetname" appended to 
the INPUT path.

=item allsheets => BOOL

Valid only with C<< cvt_to => 'csv' >>.   All sheets in the input spreadsheet
are converted to separate .csv files.  C<outpath>, if specified, must be
a directory, which will be created if necessary; 
if not specified, then a new sub-directory of I<tempdir> will
be created to contain the the resulting .csv files.

=item verbose => BOOL

=item use_gnumeric => BOOL   # instead of libre/openoffice

=item tempdir => "/path/to/dir" 

If C<tempdir> is not specified a temporary directory will be created and
auto-removed when your process exits.

=back

=head2 sane_CSV_read_options

=head2 sane_CSV_write_options

@Spreadsheet::Edit::IO:sane_CSV_read_options contains the options you
will always want to use with Text::CSV objects->new().

Specifically, quotes and embedded newlines are handled correctly.

You can append overrides,
e.g. C<auto_diag> or C<allow_whitespace>.
If you change C<quote_char>, then C<escape_char> must be set to the same value.

=head2 cx2let COLUMNINDEX

=head2 let2cx LETTERCODE

Functions which translate between spreadsheet-column
letter codes ("A", "B", etc.) and 0-based column indicies.

=head2 filepath_from_spec EXPR

=head2 sheetname_from_spec EXPR

Functions which decompose strings giving a spreadsheet path and possibly sheetname
as "FILEPATH!SHEETNAME", "FILEPATH|||SHEETNAME", or "FILEPATH[SHEETNAME]".
C<sheetname_from_spec> returns C<undef> if the input does not have a
a sheetname suffix.

=head2 form_spec_with_sheetname(PATH, SHEENAME)

Composes a combined string in a "preferred" format ("PATH!SHEETNAME" or one of the others;
which is not specified).

=cut


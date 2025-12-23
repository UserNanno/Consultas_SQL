Windows
main branch, 3.05 and later
Using Tesseract
!!! IMPORTANT !!! To use Tesseract in your application (to include Tesseract or to link it into your app) see this very simple example.

Build the latest library (using Software Network client)
Download the latest SW (Software Network https://software-network.org/) client from https://software-network.org/client/.
Run sw setup (may require administrator access)
Run sw build org.sw.demo.google.tesseract.tesseract.
Build training tools
Today it is possible to build a full set of Tesseract training tools on Windows with Visual Studio. You need to have the latest VS compiler (VS2019/2022 or light VS 2019/2022 build tools distro installed.

To do this:

Download the latest SW (Software Network https://software-network.org/client/) client from https://software-network.org/client/.
Checkout tesseract sources git clone https://github.com/tesseract-ocr/tesseract tesseract && cd tesseract.
Run sw build.
Binaries will be available under .sw\out\some hash dir...
For Visual Studio project using Tesseract (vcpkg build)
Setup Vcpkg the Visual C++ Package Manager.
Run vcpkg install tesseract:x64-windows for 64-bit. Use –head for the master branch.
Static linking
To build a self-contained tesseract.exe executable (without any DLLs or runtime dependencies), use Vcpkg as above with the following command:

vcpkg install tesseract:x64-windows-static for 64-bit
vcpkg install tesseract:x86-windows-static for 32-bit
Use –head for the main branch. It may still require one DLL for the OpenMP runtime, vcomp140.dll (which you can find in the Visual C++ Redistributable 2015).

CMake build with VS2017 without using Software Network client
Build and install Leptonica based as described on its wiki
Install ICU library for Visual Studio
chdir tesseract
cmake -Bbuild -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=%INSTALL_DIR% -DCMAKE_INSTALL_PREFIX=%INSTALL_DIR% -DSW_BUILD=OFF -DBUILD_SHARED_LIBS=ON -DENABLE_LTO=ON -DBUILD_TRAINING_TOOLS=ON -DFAST_FLOAT=ON -DGRAPHICS_DISABLED=ON -DOPENMP_BUILD=OFF
cmake --build build --config Release --target install
This will create most of the training tools (excluding text2image as its requirements Pango library is not easy to build&installed on Windows). For more details have a look at https://github.com/tesseract-ocr/tesseract/blob/main/.github/workflows/cmake-win64.yml

Develop Tesseract
For development purposes of Tesseract itself do the next steps:

Download and install Git, CMake and put them in PATH.
Download the latest SW (Software Network https://software-network.org/) client from https://software-network.org/client/. SW is a source package distribution system.
Add SW client to PATH.
Run sw setup (may require administrator access)
If you have a release archive, unpack it to tesseract dir.
If you’re using the main branch run

   git clone https://github.com/tesseract-ocr/tesseract tesseract
Run

 cd tesseract
 mkdir build && cd build
 cmake ..
Build a solution (tesseract.sln) in your Visual Studio version. If you want to build and install from the command line (e.g. Release build) you can use this command:

cmake --build . --config Release --target install
If you want to install to another directory than C:\Program Files (you will need admin right for this), you need to specify the install path during configuration:

cmake .. -G "Visual Studio 15 2017 Win64" -DCMAKE_INSTALL_PREFIX=inst
For development purposes of training tools after cloning a repo from the previous paragraph, run

sw build
You’ll see a solution link appearing in the root directory of Tesseract.

Building for x64 platform
sw
If you’re building with sw+cmake, run cmake as follows:

mkdir win64 && cd win64
cmake .. -G "Visual Studio 14 2015 Win64"
If you’re building with sw run sw generate, it will create a solution link for you (not yet implemented!).

3.05
If you have Visual Studio 2015, checkout the https://github.com/peirick/VS2015_Tesseract repository for Visual Studio 2015 Projects for Tessearct and dependencies. and click on build_tesseract.bat. After that you still need to download the language packs.

3.03rc-1
Have a look at blog How to build Tesseract 3.03 with Visual Studio 2013.

3.02
For tesseract-ocr 3.02 please follow instruction in Visual Studio 2008 Developer Notes for Tesseract-OCR.

3.01
Download these packages from the Downloads Archive on SourceForge page:

tesseract-3.01.tar.gz - Tesseract source
tesseract-3.01-win_vs.zip - Visual studio (2008 & 2010) solution with necessary libraries
tesseract-ocr-3.01.eng.tar.gz - English language file for Tesseract (or download other language training file)
Unpack them to one directory (e.g. tesseract-3.01). Note that tesseract-ocr-3.01.eng.tar.gz names the root directory 'tesseract-ocr' instead of 'tesseract-3.01'.

Windows relevant files are located in vs2008 directory (e.g. tesseract-3.01\vs2008). The same build process as usual applies: Open tesseract.sln with VC++Express 2008 and build all (or just Tesseract.) It should compile (in at least release mode) without having to install anything further. The dll dependencies and Leptonica are included. Output will be in tesseract-3.01\vs2008\bin (or tesseract-3.01\vs2008\bin.rd or tesseract-3.01\vs2008\bin.dbg based on configuration build).

Mingw+Msys
For Mingw+Msys have a look at blog Compiling Leptonica and Tesseract-ocr with Mingw+Msys.

Msys2
Download and install MSYS2 Installer from https://msys2.github.io/

The core packages groups you need to install if you wish to build from PKGBUILDs are:

base-devel for any building
msys2-devel for building msys2 packages
mingw-w64-i686-toolchain for building mingw32 packages
mingw-w64-x86_64-toolchain for building mingw64 packages
To build the tesseract-ocr release package, use PKGBUILD from https://github.com/Alexpux/MINGW-packages/tree/master/mingw-w64-tesseract-ocr

Cygwin
To build on Cygwin have a look at blog How to build Tesseract on Cygwin.

Tesseract as well as the training utilities for 3.04.00 onwards are available as Cygwin packages.

Tesseract specific packages to be installed:

tesseract-ocr                           3.04.01-1
tesseract-ocr-eng                       3.04-1
tesseract-training-core                 3.04-1
tesseract-training-eng                  3.04-1
tesseract-training-util                 3.04.01-1
Mingw-w64
Mingw-w64 allows building 32- or 64-bit executables for Windows. It can be used for native compilations on Windows, but also for cross compilations on Linux (which are easier and faster than native compilations). Most large Linux distributions already contain packages with the tools need for a cross build. Before building Tesseract, it is necessary to build some prerequisites.

For Debian and similar distributions (e. g. Ubuntu), the cross tools can be installed like that:

# Development environment targeting 32- and 64-bit Windows (required)
apt-get install mingw-w64
# Development tools for 32- and 64-bit Windows (optional)
apt-get install mingw-w64-tools
These prerequisites will be needed:

libpng, libtiff, zlib (binaries for Mingw-w64 available as part of the GTK+ bundles)
libicu
liblcms2
openjpeg
leptonica

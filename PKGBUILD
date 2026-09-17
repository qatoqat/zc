# Maintainer: qatoqat
pkgname=zc
pkgver=0.1.1
pkgrel=1
pkgdesc="Minimal terminal client for the ZCode agent runtime (no Electron)"
arch=('x86_64' 'aarch64')
url="https://github.com/qatoqat/zc"
license=('MIT')
depends=('gcc-libs' 'nodejs' 'curl')
optdepends=('zcode-bin: the ZCode runtime this client drives')
makedepends=('cargo' 'git')
source=("$pkgname-$pkgver.tar.gz::$url/archive/refs/tags/v$pkgver.tar.gz")
sha256sums=('92eb363e43be687812eab657eca170b037df99a5651e3303ed5fd02859987fab')

prepare() {
  cd "$pkgname-$pkgver"
  export RUSTUP_TOOLCHAIN=stable
  cargo fetch --locked --target "$(rustc -vV | sed -n 's/host: //p')"
}

build() {
  cd "$pkgname-$pkgver"
  export RUSTUP_TOOLCHAIN=stable
  export CARGO_TARGET_DIR=target
  cargo build --frozen --release
}

package() {
  cd "$pkgname-$pkgver"
  install -Dm755 "target/release/$pkgname" "$pkgdir/usr/bin/$pkgname"
  install -Dm644 LICENSE "$pkgdir/usr/share/licenses/$pkgname/LICENSE"
  install -Dm644 README.md "$pkgdir/usr/share/doc/$pkgname/README.md"
}

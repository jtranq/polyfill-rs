# From the repository root:
# cargo build --release --locked --example orderbook
# vhs docs/orderbook-demo.tape
Output assets/orderbook-demo.gif
Require cargo

Set Shell "bash"
Set FontFamily "Menlo"
Set FontSize 17
Set Width 1000
Set Height 560
Set Padding 20
Set Theme "Catppuccin Mocha"
Set Framerate 30
Set TypingSpeed 25ms
Set WindowBar Colorful

Hide
Type "unset NO_COLOR; export PS1='$ '"
Enter
Sleep 200ms
Type "clear"
Enter
Sleep 200ms
Type "cargo run --quiet --release --locked --example orderbook"
Enter
# Wait for a live book before recording; keep playback at real speed.
Wait+Screen@30s /LIVE/
Sleep 3s
Show
Sleep 22s
Hide
Ctrl+C
Sleep 500ms

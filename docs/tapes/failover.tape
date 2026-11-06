# Records docs/images/failover.gif with vhs. Start the cluster first: make cluster-up
Output docs/images/failover.gif
Set Shell "bash"
Set FontSize 20
Set Padding 20
Set Theme "Catppuccin Mocha"
Set TypingSpeed 30ms
Set Height 780
Set Width 1200
Env BASH_SILENCE_DEPRECATION_WARNING "1"

Hide
Type 'export PS1="\[\e[1;35m\]scheduler\[\e[0m\] $ "; make jobs COUNT=0 >/dev/null; clear' Enter
Sleep 8s
Show
Type "make jobs" Sleep 300ms Enter
Sleep 4s
Type "make cluster-status" Sleep 300ms Enter
Sleep 4s
Type "make kill-leader" Sleep 300ms Enter
Sleep 2s
Type "sleep 3 && make cluster-status" Sleep 300ms Enter
Sleep 6s
Type "# a standby took over with a higher epoch; no job moved" Enter
Sleep 2s
Type "make kill-worker" Sleep 300ms Enter
Sleep 2s
Type "sleep 8 && make cluster-status" Sleep 300ms Enter
Sleep 11s
Type "# its jobs moved to another worker; none is on two workers" Enter
Sleep 4s

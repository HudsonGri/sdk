# Machines adapter

The `machines` feature bridges sandbox and checkpoint references to a
`MachinesProvider`. `MachinesHost` captures selected forkable checkpoints and
verifies their retained provider identity before admission.

With `machines`, `filesystem` and `native-execution`, `MachineShell` binds the stock
shell tool to a selected sandbox, normalized POSIX workspace and signed task scope.
It writes the same private-volume request, streamed stdout/stderr and terminal
metadata as the native shell. Command expiry and combined output allowance travel
through the Machines seam; typed stops map to the native `ToolResult.stop` shape.
The output volume must belong to the signed scope's agent. Qualify the actual
tool calls with the provider examples' `shell-conformance` feature; all examples
use the same vectors for publication before exit, stops and cancellation on drop.
The binding uses the machine's configured disk workspace. It does not create a
kernel filesystem projection or import task volumes.

Shell parity is incomplete while the Machines command admission retains its
60-second timeout and 4096-byte argument caps: the native shell defaults to 300
seconds and accepts longer source. Calls exceeding those bounds fail admission.

package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/creack/pty"
	"golang.org/x/term"
)

const (
	maxLogBytes = 1000
	truncNote   = "(..)"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: wrapper <recording> <command> [args...]")
		os.Exit(1)
	}

	recording, command, args := os.Args[1], os.Args[2], os.Args[3:]

	isInteractive := term.IsTerminal(int(os.Stdin.Fd())) &&
		term.IsTerminal(int(os.Stdout.Fd()))

	// Open output files
	outF, err := os.Create(recording)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open output: %v\n", err)
		os.Exit(1)
	}
	tmF, err := os.Create(recording + ".tm")
	if err != nil {
		fmt.Fprintf(os.Stderr, "open timing: %v\n", err)
		os.Exit(1)
	}

	defer outF.Close()
	defer tmF.Close()

	start := time.Now()
	fmt.Fprintf(outF, "Script started on %s\n", start.Format(time.RFC3339))

	cmd := exec.Command(command, args...)

	// Ensure we always write the "Script done" line
	defer func() {
		exitCode := 0
		if cmd.ProcessState != nil {
			exitCode = cmd.ProcessState.ExitCode()
		}

		fmt.Fprintf(outF, "\nScript done on %s [CODE=%d]\n",
			time.Now().Format(time.RFC3339),
			exitCode)
	}()

	if !isInteractive {

		stdout, err := cmd.StdoutPipe()
		if err != nil {
			fmt.Fprintf(os.Stderr, "stdout pipe: %v\n", err)
			os.Exit(1)
		}
		stderr, err := cmd.StderrPipe()
		if err != nil {
			fmt.Fprintf(os.Stderr, "stderr pipe: %v\n", err)
			os.Exit(1)
		}
		stdin, err := cmd.StdinPipe()
		if err != nil {
			fmt.Fprintf(os.Stderr, "stdin pipe: %v\n", err)
			os.Exit(1)
		}

		if err := cmd.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "start: %v\n", err)
			os.Exit(1)
		}

		// Shared counter for total bytes logged
		totalLogged := 0

		// ==== Stdin: Record and pass through to command ====
		last := time.Now()
		stdinDone := make(chan struct{})
		go func() {
			buf := make([]byte, 32*1024)
			for {
				n, err := os.Stdin.Read(buf)
				if n > 0 {
					chunk := buf[:n]
					stdin.Write(chunk)

					// Limit logged data to maxLogBytes
					if totalLogged < maxLogBytes {
						remaining := maxLogBytes - totalLogged
						if n <= remaining {
							outF.Write([]byte("STDIN: "))
							outF.Write(chunk)
							outF.Write([]byte("\n"))
							totalLogged += n + 8 // +8 for "STDIN: " and newline
						} else {
							outF.Write([]byte("STDIN: "))
							outF.Write(chunk[:remaining-8]) // -8 to account for "STDIN: " and newline
							outF.WriteString(truncNote)
							outF.Write([]byte("\n"))
							totalLogged = maxLogBytes
						}
					}

					now := time.Now()
					fmt.Fprintf(tmF, "%f %d\n", now.Sub(last).Seconds(), len(chunk))
					last = now
				}
				if err != nil {
					break
				}
			}
			stdin.Close()
			stdinDone <- struct{}{}
		}()

		// ==== Stdout: Log + Timing (with log size limit) ====
		stdoutDone := make(chan struct{})
		go func() {
			buf := make([]byte, 32*1024)
			for {
				n, err := stdout.Read(buf)
				if n > 0 {
					chunk := buf[:n]
					os.Stdout.Write(chunk)

					// Limit logged data to maxLogBytes
					if totalLogged < maxLogBytes {
						remaining := maxLogBytes - totalLogged
						if n+8 <= remaining { // +8 for "STDOUT: " and newline
							outF.Write([]byte("STDOUT: "))
							outF.Write(chunk)
							outF.Write([]byte("\n"))
							totalLogged += n + 8
						} else if remaining > 8 {
							outF.Write([]byte("STDOUT: "))
							outF.Write(chunk[:remaining-8]) // -8 to account for "STDOUT: " and newline
							outF.WriteString(truncNote)
							outF.Write([]byte("\n"))
							totalLogged = maxLogBytes
						}
					}

					now := time.Now()
					fmt.Fprintf(tmF, "%f %d\n", now.Sub(last).Seconds(), len(chunk))
					last = now
				}
				if err != nil {
					break
				}
			}
			stdoutDone <- struct{}{}
		}()

		// ==== Stderr: Pass through to os.Stderr ====
		go io.Copy(os.Stderr, stderr)

		// Wait for both stdin and stdout to complete
		<-stdoutDone
		<-stdinDone

	} else {
		// Interactive PTY
		ptyF, err := pty.Start(cmd)
		if err != nil {
			fmt.Fprintf(os.Stderr, "pty start: %v\n", err)
			os.Exit(1)
		}
		defer ptyF.Close()

		// Resize handling
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGWINCH)
		go func() {
			for range sigs {
				pty.InheritSize(os.Stdin, ptyF)
			}
		}()
		sigs <- syscall.SIGWINCH
		defer signal.Stop(sigs)

		oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
		if err != nil {
			fmt.Fprintf(os.Stderr, "make raw: %v\n", err)
			os.Exit(1)
		}
		defer term.Restore(int(os.Stdin.Fd()), oldState)

		last := time.Now()

		// PTY reader (with log size limit)
		go func() {
			buf := make([]byte, 32*1024)
			for {
				n, err := ptyF.Read(buf)
				if n > 0 {
					data := buf[:n]
					os.Stdout.Write(data)

					// For interactive mode, record all output without truncation
					outF.Write(data)

					now := time.Now()
					fmt.Fprintf(tmF, "%f %d\n", now.Sub(last).Seconds(), len(data))
					last = now
				}
				if err != nil {
					break
				}
			}
		}()

		// PTY writer (interactive input)
		go io.Copy(ptyF, os.Stdin)

	}

	cmd.Wait()
}

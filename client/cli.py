import argparse
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import client.logger as log
from client.core import DistributedClient

# ── ANSI helpers (reuse from logger) ─────────────────────────────────────────
BOLD    = "\033[1m"
CYAN    = "\033[36m"
BGREEN  = "\033[92m"
YELLOW  = "\033[33m"
DIM     = "\033[2m"
RESET   = "\033[0m"

def _print_banner(server_ip, verbose):
    print()
    print(f"  {BOLD}{CYAN}╔══════════════════════════════════════════════════════════════════╗{RESET}")
    print(f"  {BOLD}{CYAN}║         Distributed File Storage System  ·  Client Node         ║{RESET}")
    print(f"  {BOLD}{CYAN}╚══════════════════════════════════════════════════════════════════╝{RESET}")
    print()
    print(f"  {DIM}Architecture : 3-Tier Client  ←→  Central Server  ←→  Peer Nodes{RESET}")
    print(f"  {DIM}Metadata Hub : {server_ip}{RESET}")
    print(f"  {DIM}Verbose Mode : {'ON  (use --quiet to disable)' if verbose else 'OFF (run without --quiet to enable)'}{RESET}")
    print()

def _help():
    print(f"""
  {BOLD}Available Commands:{RESET}
  {CYAN}  register{RESET}           Link this node with the Central Metadata Hub
  {CYAN}  upload <file>{RESET}      Publish a local file to the distributed network
  {CYAN}  download <file>{RESET}    Fetch a file from the network to local disk
  {CYAN}  delete <file>{RESET}      Permanently revoke and destroy a file you uploaded
  {CYAN}  list_files{RESET}         Show all files registered in the hub metadata index
  {CYAN}  status{RESET}             Show this node's local diagnostics + hub connectivity
  {CYAN}  quit{RESET}               Disconnect gracefully
    """)

def main():
    parser = argparse.ArgumentParser(description="Distributed File Storage Network — Client CLI")
    parser.add_argument("--storage", type=str, default="storage/default",
                        help="Local directory for storing chunk data (default: storage/default)")
    parser.add_argument("--ip", type=str, default="smart-distributed-file-storage-system.onrender.com",
                        help="Central Metadata Hub address (default: Render cloud)")
    parser.add_argument("--quiet", action="store_true",
                        help="Disable verbose architectural trace output")

    args = parser.parse_args()

    # Apply verbose setting globally before anything prints
    log.set_verbose(not args.quiet)

    _print_banner(args.ip, not args.quiet)

    client = DistributedClient(
        storage_dir=args.storage,
        server_ip=args.ip
    )

    print(f"\n  {BGREEN}{BOLD}Node ready. Type 'help' for commands, or 'register' to go online.{RESET}\n")

    # ── REPL Loop ─────────────────────────────────────────────────────────────
    while True:
        try:
            raw = input(f"  {BOLD}{CYAN}[DFS]{RESET} ❯ ").strip()
            if not raw:
                continue

            parts = raw.split(maxsplit=1)
            cmd = parts[0].lower()
            arg = parts[1].strip("'\"") if len(parts) > 1 else ""

            if cmd == "register":
                client.register()

            elif cmd == "upload":
                if not arg:
                    print(f"  {YELLOW}Usage: upload <file_path>{RESET}")
                else:
                    client.upload(arg)

            elif cmd == "download":
                if not arg:
                    print(f"  {YELLOW}Usage: download <file_name>{RESET}")
                else:
                    client.download(arg)

            elif cmd == "delete":
                if not arg:
                    print(f"  {YELLOW}Usage: delete <file_name>{RESET}")
                else:
                    client.delete(arg)

            elif cmd == "list_files":
                client.list_files()

            elif cmd == "status":
                client.status()

            elif cmd in ("help", "?"):
                _help()

            elif cmd in ("quit", "exit"):
                print(f"\n  {BGREEN}Disconnecting node gracefully. Goodbye!{RESET}\n")
                break

            else:
                print(f"  {YELLOW}Unknown command '{cmd}'. Type 'help' to see available commands.{RESET}")

        except (KeyboardInterrupt, EOFError):
            print(f"\n  {YELLOW}Force quitting terminal...{RESET}\n")
            break
        except Exception as e:
            print(f"  {YELLOW}Error: {e}{RESET}")

if __name__ == "__main__":
    main()

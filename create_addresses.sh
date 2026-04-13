#!/bin/bash

# Check if the user provided an argument N
if [ -z "$1" ]; then
    echo "Usage: sudo $0 <N>"
    echo "Example: sudo $0 255 (Adds IPs from ::0 to ::ff)"
    exit 1
fi

# The number to loop up to (decimal input)
N=$1

# Check if the script is running as root
if [[ $EUID -ne 0 ]]; then
   echo "This script must be run as root (use sudo)."
   exit 1
fi

# Configuration
# Get then with "ip addr show"
INTERFACE="" # eg: eth0, enp3s0, etc.
PREFIX="" # eg: 2001:db8:abcd:0012 (without the ::/64 part)

if [[ "$INTERFACE" == "" || "$PREFIX" == "" ]]; then
    echo "Please set the INTERFACE and PREFIX variables in the script before running."
    exit 1
fi

echo "Starting to add IPv6 addresses to $INTERFACE using Hexadecimal indexing..."

rm -f addresses.txt
for (( i=0; i<=$N; i++ ))
do
    # Convert the decimal loop index 'i' to a lowercase hexadecimal string
    HEX_VAL=$(printf "%x" $i)
    
    # Construct the full address
    # This will result in ...::0, ...::1 ... ...::a, ...::b ... ...::10, etc.
    IP_ADDR="${PREFIX}::${HEX_VAL}/64"
    
    echo "Adding: $IP_ADDR (Decimal index: $i)"
    
    # Execute the command
    ip -6 addr add "$IP_ADDR" dev "$INTERFACE" 2>/dev/null

    # Check if the command succeeded
    if [ $? -ne 0 ]; then
        echo "  [!] Error: Failed to add $IP_ADDR. (It may already exist)"
    fi
    echo "${PREFIX}::${HEX_VAL}" >> addresses.txt
done

echo "------------------------------------------"
echo "Finished. Current addresses on $INTERFACE:"
ip -6 addr show "$INTERFACE" | grep "inet6"

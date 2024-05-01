#! /bin/bash


ROUNDS=1000
GETS=2
PUTS=1
KEYS=5
#Create list of number of nodes from 3 to 9 with intervals of 2
NODES=${NODES:-"5"}
REP_FACTOR=${REP_FACTOR:-"5"}
QUORUM=${QUORUM:-"3"}
CLIENTS=${CLIENTS:-"3"}
DELAY=${DELAY:-"5"}

# Run the mix test with a combination of all these factors
for nodes in $NODES; do
    for rep_factor in $REP_FACTOR; do
        if [ $rep_factor -gt $nodes ]; then 
            continue
        fi
        for quorum in $QUORUM; do
            #Check that quorum is less than or equal to rep_factor
            if [ $quorum -gt $rep_factor ]; then
                continue
            fi
            for clients in $CLIENTS; do
                for delay in $DELAY; do
                # Run the test with the given parameters [rounds, gets, puts, keys, rep_factor, r_quorum, w_quorum, nodes, clients, delay]
                echo "Running test with rounds: $ROUNDS, gets: $GETS, puts: $PUTS, keys: $KEYS, rep_factor: $rep_factor, r_quorum: $quorum, w_quorum: $quorum, nodes: $nodes, clients: $clients, delay: $delay"
                mix test test/test_cases_var.exs --trace $ROUNDS $GETS $PUTS $KEYS $rep_factor $quorum $quorum $nodes $clients $delay
                done
            done
        done
    done
done


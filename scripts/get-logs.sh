#!/bin/bash

# Create a directory to store logs with a timestamp
timestamp=$(date +"%Y%m%d%H%M%S")
log_dir="kube_logs/logs_$timestamp"
mkdir -p "$log_dir"

# Get a list of all pods
pods=$(kubectl get pods --all-namespaces -o jsonpath='{range .items[*]}{@.metadata.namespace}:{@.metadata.name}:{@.status.phase}{"\n"}{end}')

# Loop through each pod and fetch logs
for pod in $pods; do
    echo "saving logs for $pod"
    namespace=$(echo $pod | cut -d: -f1)
    pod_name=$(echo $pod | cut -d: -f2)
    pod_phase=$(echo $pod | cut -d: -f3)

    mkdir -p "$log_dir/$namespace/logs"
    mkdir -p "$log_dir/$namespace/describe"

    # Create a directory for the pod within the timestamped folder
    pod_log="$log_dir/$namespace/logs/$pod_name.log"
    pod_previous_log="$log_dir/$namespace/logs/$pod_name-previous.log"
    pod_describe="$log_dir/$namespace/describe/$pod_name.txt"
    

    # Get the restart count for the pod
    restart_count=$(kubectl get pod "$pod_name" --namespace="$namespace" -o jsonpath='{.status.containerStatuses[0].restartCount}')

    # Check if the pod has restarted
    if [ "$restart_count" -gt 0 ]; then
        # Get logs for the pod and save to a file, including previous logs
        kubectl logs --namespace="$namespace" "$pod_name" --all-containers --previous >"$pod_previous_log" 2>&1

        echo "Logs (including previous) for $pod_name saved to $pod_previous_log"
    fi

    # If the pod is in a pending state, get a describe of the pod
    if [ "$pod_phase" == "Running" ]; then
        # Get logs for the pod and save to a file
        kubectl logs --namespace="$namespace" "$pod_name" --all-containers >"$pod_log" 2>&1

        echo "Logs for $pod_name saved to $pod_dir/logs.log"
    fi

    kubectl describe pod "$pod_name" --namespace="$namespace" >"$pod_describe" 2>&1
    echo "Describe for $pod_name saved to $pod_describe"
done

echo "Logs retrieval complete. Logs stored in folder: $log_dir"

## build image



`#: IMAGE_TAG=v0.11 IMAGE_NAMESPACE=localhost:30500 make sqs-image`  
`#: IMAGE_TAG=v0.11 IMAGE_NAMESPACE=localhost:30500 make resource-image`  


`while (true); do sleep 5 &&  kubectl logs -n argo-events resource-gateway -c resource-events -f; done`
`IMAGE_TAG=v0.11 IMAGE_NAMESPACE=localhost:30500 make resource-image && kubectl delete po -n argo-events resource-gateway`


## Message Body ex

`{"customargs":"echo hi && sleep 120 && exit 0"}`

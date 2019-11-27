/*
Copyright 2018 BlackRock, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package resource

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/argoproj/argo-events/common"
	"github.com/argoproj/argo-events/gateways"
	gwcommon "github.com/argoproj/argo-events/gateways/common"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
)

type Item struct {
	EventSource    string
	CorrelationID  string
	Payload        string
	Status         string
	StatusBody     string
	TimeoutSeconds int
	CreatedOn      string
	Resources      []Resource
}

type Resource struct {
	Link       string
	ID         string
	Status     string
	StatusBody string
}

// StartEventSource starts an event source
func (ese *ResourceEventSourceExecutor) StartEventSource(eventSource *gateways.EventSource, eventStream gateways.Eventing_StartEventSourceServer) error {
	defer gateways.Recover(eventSource.Name)

	log := ese.Log.WithField(common.LabelEventSource, eventSource.Name)
	log.Info("operating on event source")

	config, err := parseEventSource(eventSource.Data)
	if err != nil {
		log.WithError(err).Error("failed to parse event source")
		return err
	}

	dataCh := make(chan []byte)
	errorCh := make(chan error)
	doneCh := make(chan struct{}, 1)

	go ese.listenEvents(config.(*resource), eventSource, dataCh, errorCh, doneCh)

	return gateways.HandleEventsFromEventSource(eventSource.Name, eventStream, dataCh, errorCh, doneCh, ese.Log)
}

// listenEvents watches resource updates and consume those events
func (ese *ResourceEventSourceExecutor) listenEvents(s *resource, eventSource *gateways.EventSource, dataCh chan []byte, errorCh chan error, doneCh chan struct{}) {
	var awsSession *session.Session

	dbStatusCheckTicker := time.NewTicker(10 * time.Second)
	client, err := dynamic.NewForConfig(ese.K8RestConfig)
	if err != nil {
		errorCh <- err
		return
	}

	if s.AccessKey == nil && s.SecretKey == nil {
		awsSessionWithoutCreds, err := gwcommon.GetAWSSessionWithoutCreds(s.Region)
		if err != nil {
			errorCh <- err
			return
		}

		awsSession = awsSessionWithoutCreds
	} else {
		creds, err := gwcommon.GetAWSCreds(ese.Clientset, ese.Namespace, s.AccessKey, s.SecretKey)
		if err != nil {
			errorCh <- err
			return
		}

		awsSessionWithCreds, err := gwcommon.GetAWSSession(creds, s.Region)
		if err != nil {
			errorCh <- err
			return
		}

		awsSession = awsSessionWithCreds
	}

	gvr := schema.GroupVersionResource{
		Group:    s.Group,
		Version:  s.Version,
		Resource: s.Resource,
	}

	client.Resource(gvr)
	dbClient := dynamodb.New(awsSession)

	options := &metav1.ListOptions{}

	// if resourceCfg.Filter != nil && resourceCfg.Filter.Labels != nil {
	// 	sel, err := LabelSelector(resourceCfg.Filter.Labels)
	// 	if err != nil {
	// 		errorCh <- err
	// 		return
	// 	}
	// 	options.LabelSelector = sel.String()
	// }

	sel, err := controlLabel()
	if err != nil {
		errorCh <- err
		return
	}
	options.LabelSelector = sel.String()

	if s.Filter != nil && s.Filter.Fields != nil {
		sel, err := LabelSelector(s.Filter.Fields)
		if err != nil {
			errorCh <- err
			return
		}
		options.FieldSelector = sel.String()
	}

	tweakListOptions := func(op *metav1.ListOptions) {
		*op = *options
	}

	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(client, 0, s.Namespace, tweakListOptions)

	informer := factory.ForResource(gvr)

	informerEventCh := make(chan *InformerEvent)
	ese.Log.Info("Loaded the eventsource!")
	go func() {
		for {
			select {
			case _ = <-dbStatusCheckTicker.C:
				ese.Log.Infoln("checking database to update")
				err = updateStatusFromDb(dbClient)
				if err != nil {
					errorCh <- err
				}
				ese.Log.Infoln("done checking database to update")
			case event, ok := <-informerEventCh:
				if !ok {
					return
				}
				// ese.Log.Infof("%+v", event.Obj)
				eventBody, err := json.Marshal(event)
				if err != nil {
					ese.Log.WithField(common.LabelEventSource, eventSource.Name).WithError(err).Errorln("failed to parse event from resource informer")
					continue
				}
				if err := passFilters(dbClient, event.Obj.(*unstructured.Unstructured), s.Filter); err != nil {
					ese.Log.WithField(common.LabelEventSource, eventSource.Name).WithError(err).Warnln("failed to apply the filter")
					continue
				}
				dataCh <- eventBody
			}
		}
	}()

	sharedInformer := informer.Informer()
	informer.Lister()
	sharedInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				informerEventCh <- &InformerEvent{
					Obj:  obj,
					Type: ADD,
				}
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				informerEventCh <- &InformerEvent{
					Obj:    newObj,
					OldObj: oldObj,
					Type:   UPDATE,
				}
			},
			DeleteFunc: func(obj interface{}) {
				informerEventCh <- &InformerEvent{
					Obj:  obj,
					Type: DELETE,
				}
			},
		},
	)

	sharedInformer.Run(doneCh)
	ese.Log.WithField(common.LabelEventSource, eventSource.Name).Infoln("resource informer is stopped")
	close(informerEventCh)
	close(doneCh)
}

// LabelReq returns label requirements
func LabelReq(key, value string) (*labels.Requirement, error) {
	req, err := labels.NewRequirement(key, selection.Equals, []string{value})
	if err != nil {
		return nil, err
	}
	return req, nil
}

// LabelSelector returns label selector for resource filtering
func LabelSelector(resourceLabels map[string]string) (labels.Selector, error) {
	var labelRequirements []labels.Requirement
	for key, value := range resourceLabels {
		req, err := LabelReq(key, value)
		if err != nil {
			return nil, err
		}
		labelRequirements = append(labelRequirements, *req)
	}
	return labels.NewSelector().Add(labelRequirements...), nil
}

// FieldSelector returns field selector for resource filtering
func FieldSelector(fieldSelectors map[string]string) (fields.Selector, error) {
	var selectors []fields.Selector
	for key, value := range fieldSelectors {
		selector, err := fields.ParseSelector(fmt.Sprintf("%s=%s", key, value))
		if err != nil {
			return nil, err
		}
		selectors = append(selectors, selector)
	}
	return fields.AndSelectors(selectors...), nil
}

// helper method to check if the object passed the user defined filters
func passFilters(dbClient *dynamodb.DynamoDB, obj *unstructured.Unstructured, filter *ResourceFilter) error {
	// no filters are applied.
	// if filter == nil {
	// 	return nil
	// }

	// if !strings.HasPrefix(obj.GetName(), filter.Prefix) {
	// 	return errors.Errorf("resource name does not match prefix. resource-name: %s, prefix: %s", obj.GetName(), filter.Prefix)
	// }
	// created := obj.GetCreationTimestamp()
	// if !filter.CreatedBy.IsZero() && created.UTC().After(filter.CreatedBy.UTC()) {
	// 	return errors.Errorf("resource is created after filter time. creation-timestamp: %s, filter-creation-timestamp: %s", created.UTC().String(), filter.CreatedBy.UTC().String())
	// }
	labels := obj.GetLabels()
	correlationId := labels["events/correlationId"]
	tableName := "bchase-eventstatepoc"

	// filt := expression.Name("CorrelationID").Equal(expression.Value(correlationId)).And(expression.Name("State").NotEqual(expression.Value("Complete")))
	filt := expression.Name("CorrelationID").Equal(expression.Value(correlationId))

	expr, err := expression.NewBuilder().WithFilter(filt).Build()
	if err != nil {
		fmt.Println("Got error building expression:")
		fmt.Println(err.Error())
		os.Exit(1)
	}

	// Build the query input parameters
	params := &dynamodb.ScanInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		TableName:                 aws.String(tableName),
	}

	// Make the DynamoDB Query API call
	result, err := dbClient.Scan(params)
	if err != nil {
		fmt.Println("Query API call failed:")
		fmt.Println((err.Error()))
		return err
	}

	for _, i := range result.Items {
		item := Item{}
		err = dynamodbattribute.UnmarshalMap(i, &item)

		if err != nil {
			fmt.Println("Got error unmarshalling:")
			fmt.Println(err.Error())
			return err
		}
		fmt.Println("updating item: ", item)

		phase, found, err := unstructured.NestedString(obj.UnstructuredContent(), "status", "phase")
		if err != nil || !found {
			phase = "Unknown"
			//log err
		}
		statusBody, _, _ := unstructured.NestedFieldNoCopy(obj.UnstructuredContent(), "status")
		statusBodyMsg := ""
		if statusBody != nil {
			var enc []byte
			enc, err = json.Marshal(statusBody)
			if err == nil {
				statusBodyMsg = string(enc)
			}
		}
		fmt.Println("marshald status body")
		fmt.Println(statusBodyMsg)
		resourceID := string(obj.GetUID())
		resourceLink := obj.GetSelfLink()
		foundresource := false
		for i := range item.Resources {
			if item.Resources[i].ID == resourceID {
				foundresource = true
				item.Resources[i].Link = resourceLink
				item.Resources[i].Status = phase
				item.Resources[i].StatusBody = statusBodyMsg
			}
		}
		if !foundresource {
			item.Resources = append(item.Resources, Resource{ID: resourceID, Status: phase, StatusBody: statusBodyMsg})
		}

		av, err := dynamodbattribute.MarshalMap(item)
		input := &dynamodb.PutItemInput{
			Item:      av,
			TableName: aws.String(tableName),
		}
		_, err = dbClient.PutItem(input)
		if err != nil {
			fmt.Println(err.Error())
			return err
		}
	}
	return nil
}

// LabelSelector returns label selector for resource filtering
func controlLabel() (labels.Selector, error) {
	var labelRequirements []labels.Requirement
	req, err := labels.NewRequirement("events/correlationId", selection.Exists, []string{})
	if err != nil {
		return nil, err
	}
	labelRequirements = append(labelRequirements, *req)
	return labels.NewSelector().Add(labelRequirements...), nil
}

func updateStatusFromDb(dbclient *dynamodb.DynamoDB) error {

	tableName := "bchase-eventstatepoc"
	filt := expression.Name("Status").NotEqual(expression.Value("Failed")).And(expression.Name("Status").NotEqual(expression.Value("Succeeded")))

	expr, err := expression.NewBuilder().WithFilter(filt).Build()
	if err != nil {
		fmt.Println("Got error building expression:")
		fmt.Println(err.Error())
		os.Exit(1)
	}

	// Build the query input parameters
	params := &dynamodb.ScanInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		TableName:                 aws.String(tableName),
	}
	result, err := dbclient.Scan(params)
	if err != nil {
		fmt.Println("Query API call failed:")
		fmt.Println((err.Error()))
		return err
	}

	for _, i := range result.Items {
		item := Item{}
		err = dynamodbattribute.UnmarshalMap(i, &item)

		if err != nil {
			fmt.Println("Got error unmarshalling:")
			fmt.Println(err.Error())
			return err
		}
		fmt.Println("updating item: ", item)

		var itemStatus string

		//Update for timeout
		createdOn, err := time.Parse(time.RFC3339, item.CreatedOn)
		if err != nil {
			return err
		}
		expiredTime := createdOn.Add(time.Duration(item.TimeoutSeconds) * time.Second)
		now := time.Now().UTC()
		if now.After(expiredTime) {
			itemStatus = "Failed"
			itemStatusBody := fmt.Sprintf("Expired at %s. Current time when compared: $s", expiredTime, now)
			updateItemStatus(dbclient, &item, &tableName, &itemStatus, &itemStatusBody)
			return nil
		}

		//Update for resources
		if item.Resources != nil && len(item.Resources) > 0 {
			itemStatus = "Running"
			isAllComplete := true
			isAllSuccessful := true
			for _, i := range item.Resources {
				if !(i.Status == "Failed" || i.Status == "Succeeded") {
					isAllComplete = false
				}
				if i.Status == "Failed" {
					isAllSuccessful = false
				}
			}

			sbsjson := ""
			if isAllComplete {
				//Update parent status to complete
				if isAllSuccessful {
					itemStatus = "Succeeded"
				} else {
					itemStatus = "Failed"
				}

				sbs := []map[string]interface{}{}
				for _, i := range item.Resources {
					sbmap := map[string]interface{}{}
					var sb interface{}
					json.Unmarshal([]byte(i.StatusBody), &sb)
					sbmap[i.Link] = sb
					sbs = append(sbs, sbmap)
				}
				sbsb, _ := json.Marshal(sbs)
				sbsjson = string(sbsb)

			}
			updateItemStatus(dbclient, &item, &tableName, &itemStatus, &sbsjson)
		}
	}
	return nil
}

func updateItemStatus(dbclient *dynamodb.DynamoDB, item *Item, tableName, status, statusBody *string) error {

	item.Status = *status
	item.StatusBody = *statusBody

	av, err := dynamodbattribute.MarshalMap(item)
	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(*tableName),
	}
	_, err = dbclient.PutItem(input)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	return nil
}

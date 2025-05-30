/*
Copyright 2024.

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

package utils

import (
	"context"

	"github.com/nakamasato/mysql-operator/internal/constants"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// FinalizerParams contains the parameters needed for finalizer handling
type FinalizerParams struct {
	// The client object being finalized
	Object client.Object
	// The context for the operation
	Context context.Context
	// The Kubernetes client
	Client client.Client
	// The finalizer string to use
	Finalizer string
	// The specific function that contains the actual finalization logic
	FinalizationFunc func() error
	// Handler for finalization failures
	OnFailure func(error) error
}

// removeFinalizer is a helper function to remove a finalizer and update the object
func removeFinalizer(ctx context.Context, client client.Client, obj client.Object, finalizer string) error {
	log := log.FromContext(ctx).WithName("Finalizer").WithValues(
		"name", obj.GetName(),
		"namespace", obj.GetNamespace(),
		"finalizer", finalizer,
	)

	if controllerutil.RemoveFinalizer(obj, finalizer) {
		log.Info("Removing finalizer")
		if err := client.Update(ctx, obj); err != nil {
			log.Error(err, "Failed to update object after finalizer removal")
			return err
		}
		log.Info("Finalizer removed")
	}
	return nil
}

// HandleFinalizer manages the finalizer for a controller
// It returns a Result and error to be returned from the Reconcile method
// If the object is not being deleted, it adds a finalizer if needed and returns
// If the object is being deleted, it runs finalization logic and returns
// The calling controller is responsible for checking the return value and taking
// appropriate action
func HandleFinalizer(params FinalizerParams) (ctrl.Result, error) {
	log := log.FromContext(params.Context).WithName("Finalizer").WithValues(
		"name", params.Object.GetName(),
		"namespace", params.Object.GetNamespace(),
		"finalizer", params.Finalizer,
	)

	// If resource isn't being deleted, just add the finalizer if not exists
	if params.Object.GetDeletionTimestamp().IsZero() {
		log.Info("Adding finalizer to resource")
		if controllerutil.AddFinalizer(params.Object, params.Finalizer) {
			if err := params.Client.Update(params.Context, params.Object); err != nil {
				log.Error(err, "Failed to update object after adding finalizer")
				return ctrl.Result{}, err
			}
			log.Info("Successfully added finalizer to resource")
		}
		return ctrl.Result{}, nil
	}

	// Resource is being deleted
	log.Info("Resource marked for deletion, checking finalizer status")
	if !controllerutil.ContainsFinalizer(params.Object, params.Finalizer) {
		log.Info("No finalizer found, skipping finalization")
		return ctrl.Result{}, nil
	}

	// Check if abandon deletion policy is set
	if params.Object.GetAnnotations()[constants.DeletionPolicyAnnotation] == "abandon" {
		log.Info("Using abandon deletion policy, removing finalizer without cleanup")
		if err := removeFinalizer(params.Context, params.Client, params.Object, params.Finalizer); err != nil {
			log.Error(err, "Failed to remove finalizer with abandon policy")
			return ctrl.Result{}, err
		}
		log.Info("Successfully removed finalizer with abandon policy")
		return ctrl.Result{}, nil
	}

	log.Info("Starting finalization process")

	// Run the specific finalization logic
	if err := params.FinalizationFunc(); err != nil {
		log.Error(err, "Failed to execute finalization logic")

		// Call the failure handler if provided
		if params.OnFailure != nil {
			if handlerErr := params.OnFailure(err); handlerErr != nil {
				log.Error(handlerErr, "Error in failure handler")
			}
		}

		return ctrl.Result{}, err
	}

	log.Info("Finalization logic executed successfully, removing finalizer")
	if err := removeFinalizer(params.Context, params.Client, params.Object, params.Finalizer); err != nil {
		log.Error(err, "Failed to remove finalizer after successful finalization")
		return ctrl.Result{}, err
	}

	log.Info("Finalization process completed successfully")
	return ctrl.Result{}, nil
}

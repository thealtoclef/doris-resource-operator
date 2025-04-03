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
		if controllerutil.AddFinalizer(params.Object, params.Finalizer) {
			log.Info("Added finalizer")
			err := params.Client.Update(params.Context, params.Object)
			if err != nil {
				log.Error(err, "Failed to update object after adding finalizer")
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Resource is being deleted
	log.Info("Resource marked for deletion")
	if controllerutil.ContainsFinalizer(params.Object, params.Finalizer) {
		log.Info("Finalizer exists, executing finalization logic")

		// Run the specific finalization logic
		if err := params.FinalizationFunc(); err != nil {
			log.Error(err, "Failed to finalize resource")

			// Call the failure handler if provided
			if params.OnFailure != nil {
				if handlerErr := params.OnFailure(err); handlerErr != nil {
					log.Error(handlerErr, "Error in failure handler")
				}
			}

			return ctrl.Result{}, err
		}
		log.Info("Finalization completed")

		// Remove finalizer
		if controllerutil.RemoveFinalizer(params.Object, params.Finalizer) {
			log.Info("Removing finalizer")
			err := params.Client.Update(params.Context, params.Object)
			if err != nil {
				log.Error(err, "Failed to remove finalizer")
				return ctrl.Result{}, err
			}
			log.Info("Finalizer removed")
		}
	}
	return ctrl.Result{}, nil
}

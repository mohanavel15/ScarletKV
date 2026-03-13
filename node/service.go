package main

import "log"

type Service interface {
	ListenAndServe() error
	Close()
}

type ServiceManager struct {
	services map[string]Service
}

func NewServiceManager() ServiceManager {
	return ServiceManager{
		services: map[string]Service{},
	}
}

func (sm *ServiceManager) addService(name string, service Service) {
	sm.services[name] = service
}

func (sm *ServiceManager) Start() {
	for name, service := range sm.services {
		go func() {
			log.Printf("Started %s Service\n", name)
			err := service.ListenAndServe()
			if err != nil {
				log.Println("%s Service Error %v", name, err)
			}
		}()
	}
}

func (sm *ServiceManager) Stop() {
	for name, service := range sm.services {
		service.Close()

		log.Printf("Stopped %s Service\n", name)
	}
}

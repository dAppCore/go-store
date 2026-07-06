package store

func ExampleNewService() {
	_ = NewService
}

func ExampleRegister() {
	_ = Register
}

func ExampleService_OnStartup() {
	_ = (*Service).OnStartup
}

func ExampleService_OnShutdown() {
	_ = (*Service).OnShutdown
}

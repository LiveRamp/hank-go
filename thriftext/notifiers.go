package thriftext

type DataListener interface {
	OnDataChange(newVal interface{}) (err error)
}

type DataChangeNotifier interface {
	OnChange()
}

type NoOp struct{}

func (t *NoOp) OnDataChange(newVal interface{}) error { return nil }
func (t *NoOp) OnChange()                             {}

type Adapter struct {
	Notifier DataChangeNotifier
}

func (t *Adapter) OnDataChange(newVal interface{}) (err error){
	t.Notifier.OnChange()
	return nil
}

type MultiNotifier struct {
	clientListeners []DataChangeNotifier
}

func NewMultiNotifier() *MultiNotifier {
	return &MultiNotifier{clientListeners: []DataChangeNotifier{}}
}

func (p *MultiNotifier) AddClient(notifier DataChangeNotifier) {
	p.clientListeners = append(p.clientListeners, notifier)
}

func (p *MultiNotifier) OnChange() {
	for _, listener := range p.clientListeners {
		listener.OnChange()
	}
}

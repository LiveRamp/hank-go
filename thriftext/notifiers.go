package thriftext

type DataListener interface {
	OnDataChange(newVal interface{}, src string) (err error)
}

type DataChangeNotifier interface {
	OnChange(str string)
}

type NoOp struct{}

func (t *NoOp) OnDataChange(newVal interface{}, path string) error { return nil }
func (t *NoOp) OnChange(path string)                             {}

type Adapter struct {
	Notifier DataChangeNotifier
}

func (t *Adapter) OnDataChange(newVal interface{}, src string) (err error) {
	t.Notifier.OnChange(src)
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

func (p *MultiNotifier) OnChange(src string) {
	for _, listener := range p.clientListeners {
		listener.OnChange(src)
	}
}

package utils

type MiddlewareFunc[T any] func(T, func(T) T) T

type Middleware[T any] struct {
	middlewares []MiddlewareFunc[T]
}

func NewMiddleware[T any]() *Middleware[T] {
	return &Middleware[T]{
		middlewares: []MiddlewareFunc[T]{},
	}
}

func (m *Middleware[T]) Add(fn MiddlewareFunc[T]) {
	m.middlewares = append(m.middlewares, fn)
}

func (m *Middleware[T]) Execute(value T, final func(T) T) T {
	return m.run(0, value, final)
}

func (m *Middleware[T]) run(index int, value T, final func(T) T) T {
	if index >= len(m.middlewares) {
		return final(value)
	}

	current := m.middlewares[index]

	return current(value, func(nextVal T) T {
		return m.run(index+1, nextVal, final)
	})
}

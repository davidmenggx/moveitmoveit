#include "deque.hpp"

#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>

#include <exception>

namespace nb = nanobind;
using namespace moveitmoveit;

struct EmptyException : public std::exception {
  const char *what() const noexcept override { return "Queue is empty."; }
};

struct AbortException : public std::exception {
  const char *what() const noexcept override {
    return "Queue operation safely aborted, please retry.";
  }
};

struct FullException : public std::exception {
  const char *what() const noexcept override { return "Queue is full."; }
};

// RAII ObjectDescriptor wrapper
class ReleaseGuard {
public:
  ReleaseGuard(Deque &d, ObjectDescriptor desc) noexcept
      : d_(&d), desc_(desc) {}
  ReleaseGuard(const ReleaseGuard &) = delete;
  ReleaseGuard &operator=(const ReleaseGuard &) = delete;

  void reset() {
    if (d_) {
      Deque *d{d_};
      ObjectDescriptor desc{desc_};
      d_ = nullptr;
      nb::gil_scoped_release release;
      d->release(desc);
    }
  }
  ~ReleaseGuard() {
    if (d_)
      d_->release(desc_);
  }

private:
  Deque *d_;
  ObjectDescriptor desc_;
};

// Utilize Python buffer protocol
struct ObjectView {
  Deque *d_;
  ObjectDescriptor desc_;
  ~ObjectView() {
    nb::gil_scoped_release release;
    d_->release(desc_);
  }
};

static int objview_getbuffer(PyObject *self, Py_buffer *view, int flags) {
  ObjectView *v{nb::inst_ptr<ObjectView>(self)};
  void *ptr{const_cast<char *>(v->d_->get_data_ptr(v->desc_.offset_))};
  return PyBuffer_FillInfo(view, self, ptr,
                           static_cast<Py_ssize_t>(v->desc_.size_),
                           /*readonly=*/1, flags);
}

static PyType_Slot objview_slots[] = {
    {Py_bf_getbuffer, reinterpret_cast<void *>(objview_getbuffer)},
    {0, nullptr}};

NB_MODULE(moveitmoveit_ext, m) {
  nb::exception<AbortException> exc_abort(m, "Abort");
  nb::exception<EmptyException> exc_empty(m, "Empty");
  nb::exception<FullException> exc_full(m, "Full");

  nb::object pickle = nb::module_::import_("pickle");
  nb::object dumps = pickle.attr("dumps");
  nb::object loads = pickle.attr("loads");

  nb::class_<ObjectView>(m, "ObjectView", nb::type_slots(objview_slots));

  nb::class_<Deque>(m, "Deque")
      .def(nb::init<std::string, std::size_t>(), nb::arg("group_id"),
           nb::arg("total_memory_capacity_mb") = 16384)

      // --- Pickle methods ---

      .def(
          "put",
          [dumps](Deque &d, nb::handle obj) {
            nb::bytes py_bytes{nb::cast<nb::bytes>(dumps(obj))};
            const char *p{py_bytes.c_str()};
            std::size_t n{py_bytes.size()};

            nb::gil_scoped_release release;
            d.put(p, n);
          },
          nb::arg("data"))

      .def(
          "try_put",
          [dumps](Deque &d, nb::handle obj) {
            nb::bytes py_bytes{nb::cast<nb::bytes>(dumps(obj))};
            const char *p{py_bytes.c_str()};
            std::size_t n{py_bytes.size()};
            bool ok{false};

            {
              nb::gil_scoped_release release;
              ok = d.try_put(p, n);
            }

            if (!ok)
              throw FullException();
          },
          nb::arg("data"))

      .def("get",
           [loads](Deque &d) {
             ObjectDescriptor desc{ABORT};

             {
               nb::gil_scoped_release release;
               desc = d.get();
             }

             if (desc == ABORT)
               throw AbortException();
             if (desc == EMPTY)
               throw EmptyException();

             ReleaseGuard guard(d, desc);
             nb::bytes b(d.get_data_ptr(desc.offset_), desc.size_);
             guard.reset();

             return loads(b);
           })

      .def(
          "steal",
          [loads](Deque &d, bool target_longest, bool target_first) {
            ObjectDescriptor desc{ABORT};

            {
              nb::gil_scoped_release release;
              desc = d.steal(target_longest, target_first);
            }

            if (desc == ABORT)
              throw AbortException();
            if (desc == EMPTY)
              throw EmptyException();

            ReleaseGuard guard(d, desc);
            nb::bytes py_bytes(d.get_data_ptr(desc.offset_), desc.size_);
            guard.reset();

            return loads(py_bytes);
          },
          nb::arg("target_longest") = false, nb::arg("target_first") = false)

      // --- No copy methods ---

      .def(
          "put_buffer",
          [](Deque &d, nb::handle buf) {
            Py_buffer view;

            if (PyObject_GetBuffer(buf.ptr(), &view, PyBUF_SIMPLE) != 0)
              throw nb::python_error();

            struct G {
              Py_buffer *v;
              ~G() { PyBuffer_Release(v); }
            };

            G g{&view};

            const char *p{static_cast<const char *>(view.buf)};
            std::size_t n{static_cast<std::size_t>(view.len)};

            nb::gil_scoped_release release;
            d.put(p, n);
          },
          nb::arg("data"))

      .def(
          "get_view",
          [](Deque &d) -> ObjectView * {
            ObjectDescriptor desc{ABORT};

            {
              nb::gil_scoped_release release;
              desc = d.get();
            }

            if (desc == ABORT)
              throw AbortException();
            if (desc == EMPTY)
              throw EmptyException();

            return new ObjectView{&d, desc};
          },
          nb::rv_policy::take_ownership, nb::keep_alive<0, 1>())

      .def(
          "steal_view",
          [](Deque &d, bool target_longest, bool target_first) -> ObjectView * {
            ObjectDescriptor desc{ABORT};

            {
              nb::gil_scoped_release release;
              desc = d.steal(target_longest, target_first);
            }

            if (desc == ABORT)
              throw AbortException();
            if (desc == EMPTY)
              throw EmptyException();

            return new ObjectView{&d, desc};
          },
          nb::arg("target_longest") = false, nb::arg("target_first") = false,
          nb::rv_policy::take_ownership, nb::keep_alive<0, 1>())

      .def("qsize", &Deque::qsize)
      .def("empty", &Deque::empty)
      .def("full", &Deque::full);
}

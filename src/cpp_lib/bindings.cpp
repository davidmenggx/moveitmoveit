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
    return "Queue operation aborted.";
  }
};

NB_MODULE(moveitmoveit_ext, m) {
  nb::exception<EmptyException> exc_empty(m, "Empty");
  nb::exception<AbortException> exc_abort(m, "Abort");

  nb::object pickle = nb::module_::import_("pickle");

  nb::class_<Deque>(m, "Queue")
      .def(nb::init<std::string, std::size_t>(), nb::arg("group_id"),
           nb::arg("total_memory_capacity_mb") = 16384)

      .def(
          "put",
          [pickle](Deque &d, nb::handle obj) {
            nb::bytes py_bytes = nb::cast<nb::bytes>(pickle.attr("dumps")(obj));
            d.put(py_bytes.c_str(), py_bytes.size());
          },
          nb::arg("data"))

      .def(
          "try_put",
          [pickle](Deque &d, nb::handle obj) {
            nb::bytes py_bytes = nb::cast<nb::bytes>(pickle.attr("dumps")(obj));
            if (!d.try_put(py_bytes.c_str(), py_bytes.size())) {
              nb::object queue_mod = nb::module_::import_("queue");
              PyErr_SetNone(queue_mod.attr("Full").ptr());
              throw nb::python_error();
            }
          },
          nb::arg("data"))

      .def("get",
           [pickle](Deque &d) {
             ObjectDescriptor desc = d.get();

             if (desc == EMPTY)
               throw EmptyException();
             if (desc == ABORT)
               throw AbortException();

             const char *ptr = d.get_data_ptr(desc.offset_);

             nb::bytes py_bytes(ptr, desc.size_);

             d.release(desc);

             return pickle.attr("loads")(py_bytes);
           })

      .def(
          "steal",
          [pickle](Deque &d, bool target_longest) {
            ObjectDescriptor desc = d.steal(target_longest);

            if (desc == EMPTY)
              throw EmptyException();
            if (desc == ABORT)
              throw AbortException();

            const char *ptr = d.get_data_ptr(desc.offset_);
            nb::bytes py_bytes(ptr, desc.size_);
            d.release(desc);

            return pickle.attr("loads")(py_bytes);
          },
          nb::arg("target_longest") = false)

      .def("qsize", &Deque::qsize)
      .def("empty", &Deque::empty)
      .def("full", &Deque::full);
}

#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2016, S. <essepuntato@gmail.com>
#
# Permission to use, copy, modify, and/or distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
__author__ = 'essepuntato'

import argparse
import re
import rdflib
import shutil
import os
from rdflib.namespace import RDF
from rdflib import BNode
import logging

# Global variables
d2rq = rdflib.Namespace("http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#")
log = logging.getLogger("D2RParser logger")
log_h = logging.StreamHandler()
log_f = logging.Formatter('%(levelname)s - %(message)s')
log_h.setFormatter(log_f)
log.addHandler(log_h)


class D2RParser(object):
    """This class enables the parsing of directories and files containing
    D2RQ mappings and makes available methods for producing a new mapping file
    according to the file parsed."""

    def __init__(self, path_list, tmp_dir=None):
        """The constructor of the class."""
        self.path_list = path_list
        self.tmp_dir = tmp_dir
        self.final_mapping = rdflib.Graph()
        self.classmap_set = set()
        self.translationtable_set = set()
        self.database_set = set()

    @staticmethod
    def local_name(res):
        """This static method allows one to retrieve the local name of an RDF resource."""
        cur_uri = str(res)
        sharp_match = re.search("^[^#]*#(.+)$", cur_uri)
        if sharp_match:
            cur_uri = sharp_match.group(1)

        slash_match = re.search("^.*/([^/]+)$", cur_uri)
        if slash_match:
            cur_uri = slash_match.group(1)

        return cur_uri

    def parse_file(self, file_path):
        """This method allows one to parse a file containing D2RQ specifications and to
        add it to the final mapping."""
        cur_graph = D2RParser.validate_file(file_path, self.tmp_dir)

        if cur_graph:
            # Add triples to cur_graph
            for s, p, o in cur_graph:
                # If the subject of the triple is already included in any of the references
                # sets available, don't add the related triples in the final mapping, since
                # they are already there
                if s not in self.translationtable_set | self.database_set:
                    self.final_mapping.add((s, p, o))

                # Add the subject to classmap set if it is a classmap
                if o == d2rq.ClassMap:
                    self.classmap_set.add(s)

            # Add all the namespaces defined in the source graph
            self.add_declared_namespaces(cur_graph)

            # Copy references to class maps
            for cur_classmap in cur_graph.objects(None, d2rq.refersToClassMap):
                if cur_classmap not in self.classmap_set:
                    self.classmap_set.add(cur_classmap)
                    self.retrieve_references(file_path, cur_classmap)

            # Copy references to translation tables
            for cur_translationtable in cur_graph.objects(None, d2rq.translateWith):
                if cur_translationtable not in self.translationtable_set:
                    self.translationtable_set.add(cur_translationtable)
                    self.retrieve_references(file_path, cur_translationtable)

            # Copy references to databases
            for cur_database in cur_graph.objects(None, d2rq.dataStorage):
                if cur_database not in self.database_set:
                    self.database_set.add(cur_database)
                    self.retrieve_references(file_path, cur_database)

        self.clear_orphan_blank_nodes()

    def add_declared_namespaces(self, cur_graph):
        """This method adds all the namespaces declared in the graph specified
        as input in the final mapping graph."""
        for cur_ns in cur_graph.namespace_manager.namespaces():
            self.final_mapping.namespace_manager.bind(cur_ns[0], cur_ns[1])

    def retrieve_references(self, file_path, cur_reference):
        """This method checks all the possible references of a certain resource
        in the turtle file that is called like the entity itself and that must
        be contained in the same directory of an input file."""
        cur_name = D2RParser.local_name(cur_reference)
        reference_file_path = \
            D2RParser.complete_file_path(os.path.dirname(file_path) + os.sep + cur_name)
        if reference_file_path:
            cur_reference_graph = \
                D2RParser.validate_file(reference_file_path)
            if cur_reference_graph:
                self.add_declared_namespaces(cur_reference_graph)
                for p, o in cur_reference_graph.predicate_objects(cur_reference):
                    self.final_mapping.add((cur_reference, p, o))
                    if isinstance(o, BNode):
                        for o_p, o_o in cur_reference_graph.predicate_objects(o):
                            self.final_mapping.add((o, o_p, o_o))

    @staticmethod
    def complete_file_path(incomplete_file_path):
        """This method complete the path provided as input without extension."""
        for cur_dir, cur_subdir, cur_files in os.walk(os.path.dirname(incomplete_file_path)):
            for cur_file in cur_files:
                cur_path = cur_dir + os.sep + cur_file
                if cur_path.startswith(incomplete_file_path + "."):
                    return cur_path

    def log_references(self, reference_property, reference_type):
        """This method looks at the current status of the final mapping and generates
        a report for all the references that have not been solved."""
        for cur_reference in self.final_mapping.objects(None, reference_property):
            if (cur_reference, RDF.type, reference_type) not in self.final_mapping:
                log.warning(
                    "The %s '%s' was not found in the files specified." %
                    (D2RParser.local_name(reference_type), D2RParser.local_name(cur_reference)))

    def parse_path_list(self):
        """This method parse all the files and directories made available by means of the
        class constructor and update the mapping file accordingly."""
        for cur_path in self.path_list:
            if os.path.isdir(cur_path):
                for cur_dir, cur_subdir, cur_files in os.walk(cur_path):
                    for cur_file in cur_files:
                        if re.search("\.(rdf|owl|ttl|n3)$", cur_file):
                            self.parse_file(cur_dir + os.sep + cur_file)
            else:  # It's a file
                self.parse_file(cur_path)

        # Generate warning messages
        self.log_references(d2rq.refersToClassMap, d2rq.ClassMap)
        self.log_references(d2rq.translateWith, d2rq.TranslationTable)
        self.log_references(d2rq.dataStorage, d2rq.Database)

    def clear_orphan_blank_nodes(self):
        """This method removes all the triples that includes blank nodes that are not
        linked to anything"""
        for s in self.final_mapping.subjects(None, None):
            if isinstance(s, BNode) and s not in self.final_mapping.objects(None, None):
                self.final_mapping.remove((s, None, None))

    def clear_orphan_property_bridges(self):
        """This method removes all the triples related to property bridges that contain
        references that are not present in the final mapping."""
        for s, o in self.final_mapping.subject_objects(d2rq.refersToClassMap):
            self.clear_d2rq_entity(s, o)
        for s, o in self.final_mapping.subject_objects(d2rq.translateWith):
            self.clear_d2rq_entity(s, o)
        for s, o in self.final_mapping.subject_objects(d2rq.dataStorage):
            self.clear_d2rq_entity(s, o)

    def clear_d2rq_entity(self, cur_d2rq_entity, cur_reference):
        """This method removes all the D2RQ entities that links to references
        that are not declared in the final mapping."""
        if cur_reference not in self.final_mapping.subjects(RDF.type, None):
            for s, p, o in self.final_mapping.triples((cur_d2rq_entity, None, None)):
                self.final_mapping.remove((s, p, o))

    def store_mapping(self, dest_file):
        """This method stores the final mapping in a Turtle file."""
        self.clear_orphan_property_bridges()
        self.final_mapping.serialize(dest_file, format="turtle")
        log.info("Mapping file stored in '%s'." % dest_file)

    @staticmethod
    def validate_file(cur_path, tmp_dir=None):
        """This method checks if the file specified is valid according to the
        typing expected for a D2RQ module."""
        cur_graph = D2RParser.__load_graph_from_format(cur_path, tmp_dir)
        cur_name = re.sub("\..+$", "", os.path.basename(cur_path))
        found = False
        found_resource = None
        found_type = None

        for s, p, o in cur_graph.triples((None, RDF.type, None)):
            cur_local_name = D2RParser.local_name(s)
            if cur_local_name == cur_name:
                found = True
                found_resource = s
                found_type = o

        if found:
            log.info("The file '%s' is valid.\nThe main resource is '%s' and its type is '%s'." %
                     (cur_path, found_resource, found_type))
            return cur_graph
        else:
            log.warning("No resource named '%s' is defined in the file '%s'." % (cur_name, cur_path))

    @staticmethod
    def __load_graph_from_format(rdf_file_path, tmp_dir):
        """This method loads an RDF graph from the specified file."""
        current_graph = None

        if os.path.isfile(rdf_file_path):
            try:
                current_graph = D2RParser.__load_graph(rdf_file_path)
            except IOError as e:
                if tmp_dir is None:
                    raise e
                else:
                    current_file_path = tmp_dir + os.sep + "tmp_rdf_file.rdf"
                    shutil.copyfile(rdf_file_path, current_file_path)
                    current_graph = D2RParser.__load_graph(current_file_path)
                    os.remove(current_file_path)
        else:
            raise IOError("1", "The file specified doesn't exist.")

        return current_graph

    @staticmethod
    def __load_graph(file_path):
        """This method tries to load an RDF graph from the file specified as input according
        to any possible format that has been used to store the RDF statements
        (i.e., JSON-LD, RDX/XML, Turtle, and Trig)."""
        formats = ["json-ld", "rdfxml", "turtle", "trig"]

        current_graph = rdflib.Graph()

        for cur_format in formats:
            try:
                current_graph.load(file_path, format=cur_format)
                return current_graph
            except Exception:
                pass  # Try another format

        raise IOError("2", "It was impossible to handle the format used for storing the file")


# Main
if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser("d2r_mapping_manager.py")
    arg_parser.add_argument("-s", "--sources", dest="source", nargs="+",
                            help="The directory containing all the mapping files or "
                                 "a list of single files to take into account.")
    arg_parser.add_argument("-d", "--dest-file", dest="dest_file",
                            help="The file where to save the final mapping.")
    arg_parser.add_argument("-v", "--validate-file", dest="val_file",
                            help="The file to validate.")
    arg_parser.add_argument("-t", "--tmp-dir", dest="tmp_dir",
                            help="A temporary directory that will be used in case there will be "
                                 "issues in loading RDF graphs from files which have a path "
                                 "with special characters (which is a bug introduced by RDFLIB).")
    arg_parser.add_argument("-V", "--verbose", dest="debug", action="store_true",
                            help="Activate the verbose mode.")

    args = arg_parser.parse_args()

    if args.debug:
        log.setLevel(logging.DEBUG)

    if args.val_file:
        if not args.debug:
            log.setLevel(logging.INFO)
        D2RParser.validate_file(args.val_file, args.tmp_dir)
        if not args.debug:
            log.setLevel(logging.WARNING)

    if args.source and args.dest_file:
        d2r_parser = D2RParser(args.source, args.tmp_dir)
        d2r_parser.parse_path_list()
        if not args.debug:
            log.setLevel(logging.INFO)
        d2r_parser.store_mapping(args.dest_file)
        if not args.debug:
            log.setLevel(logging.WARNING)